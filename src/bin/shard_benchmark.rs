//! shard_benchmark.rs — Independent before/after benchmark for Sharded Broker
//!
//! Validates throughput differences between 1 shard and N shards.
//! Uses multiple topics to ensure messages hash to different shards.
//! Uses backpressured batching to avoid hitting the broker's 128-message
//! queue drop-oldest limit, preventing message loss from skewing results.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Instant;

// ─── Constants & Types ────────────────────────────────────────────────────────

const PT_CONNECT: u8 = 1;
const PT_CONNACK: u8 = 2;
const PT_PUBLISH: u8 = 3;
const PT_SUBSCRIBE: u8 = 8;
const PT_SUBACK: u8 = 9;
const PT_DISCONNECT: u8 = 14;

// ─── MQTT Wire Encoders (Self-contained, copied from blitzclient) ─────────────

fn encode_remaining_length(mut len: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(4);
    loop {
        let mut encoded_byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            encoded_byte |= 0x80;
        }
        out.push(encoded_byte);
        if len == 0 {
            break;
        }
    }
    out
}

fn decode_remaining_length(buf: &[u8]) -> Result<(u32, usize), String> {
    let mut value: u32 = 0;
    let mut i: usize = 0;
    loop {
        if i >= 4 {
            return Err("remaining length exceeds 4 bytes".into());
        }
        if i >= buf.len() {
            return Err("truncated remaining length field".into());
        }
        let byte = buf[i];
        let multiplier: u32 = 128u32.pow(i as u32);
        value += (byte as u32 & 0x7F) * multiplier;
        i += 1;
        if byte & 0x80 == 0 {
            break;
        }
    }
    Ok((value, i))
}

fn encode_utf8_str(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    let len = bytes.len().min(u16::MAX as usize) as u16;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&bytes[..len as usize]);
}

fn make_packet(packet_type: u8, flags: u8, body: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push((packet_type << 4) | (flags & 0x0F));
    out.extend_from_slice(&encode_remaining_length(body.len() as u32));
    out.extend_from_slice(&body);
    out
}

fn encode_connect(client_id: &str) -> Vec<u8> {
    let mut body = Vec::new();
    encode_utf8_str("MQTT", &mut body);
    body.push(4);
    body.push(0x02); // clean_session
    body.extend_from_slice(&60u16.to_be_bytes());
    encode_utf8_str(client_id, &mut body);
    make_packet(PT_CONNECT, 0, body)
}

fn encode_subscribe(packet_id: u16, filter: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    encode_utf8_str(filter, &mut body);
    body.push(0);
    make_packet(PT_SUBSCRIBE, 0b0010, body)
}

fn encode_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    encode_utf8_str(topic, &mut body);
    body.extend_from_slice(payload);
    make_packet(PT_PUBLISH, 0, body)
}

fn encode_disconnect() -> Vec<u8> {
    vec![(PT_DISCONNECT << 4), 0x00]
}

fn read_packet(stream: &mut TcpStream) -> Result<(u8, u8, Vec<u8>), String> {
    let mut header_byte = [0u8; 1];
    stream
        .read_exact(&mut header_byte)
        .map_err(|e| e.to_string())?;
    let packet_type = header_byte[0] >> 4;
    let flags = header_byte[0] & 0x0F;

    let mut rl_bytes = Vec::with_capacity(4);
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).map_err(|e| e.to_string())?;
        rl_bytes.push(b[0]);
        if b[0] & 0x80 == 0 {
            break;
        }
        if rl_bytes.len() >= 4 {
            return Err("remaining length > 4 bytes".into());
        }
    }
    let (remaining_len, _) = decode_remaining_length(&rl_bytes)?;

    let body_len = remaining_len as usize;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        stream.read_exact(&mut body).map_err(|e| e.to_string())?;
    }
    Ok((packet_type, flags, body))
}

fn connect_to_broker(host: &str, port: u16, client_id: &str) -> Result<TcpStream, String> {
    let addr = format!("{}:{}", host, port);
    let mut stream = TcpStream::connect(&addr).map_err(|e| e.to_string())?;

    stream
        .write_all(&encode_connect(client_id))
        .map_err(|e| e.to_string())?;

    let (pkt_type, _flags, body) = read_packet(&mut stream)?;
    if pkt_type != PT_CONNACK {
        return Err("Expected CONNACK".into());
    }
    if body.len() < 2 || body[1] != 0 {
        return Err("CONNACK refused".into());
    }

    Ok(stream)
}

// ─── Benchmark Logic ─────────────────────────────────────────────────────────

struct BenchConfig {
    host: String,
    port: u16,
    channels: usize,
    messages_per_channel: usize,
    batch_size: usize,
}

fn main() {
    let mut config = BenchConfig {
        host: "127.0.0.1".to_string(),
        port: 1883,
        channels: 16,
        messages_per_channel: 1000,
        batch_size: 50, // Keep below DEFAULT_CLIENT_QUEUE_CAPACITY (128)
    };

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => {
                i += 1;
                config.host = args[i].clone();
            }
            "--port" => {
                i += 1;
                config.port = args[i].parse().unwrap();
            }
            "--channels" => {
                i += 1;
                config.channels = args[i].parse().unwrap();
            }
            "--messages" => {
                i += 1;
                config.messages_per_channel = args[i].parse().unwrap();
            }
            "--batch-size" => {
                i += 1;
                config.batch_size = args[i].parse().unwrap();
            }
            _ => {}
        }
        i += 1;
    }

    println!("Starting benchmark...");
    println!("Host: {}:{}", config.host, config.port);
    println!("Channels: {}", config.channels);
    println!("Messages per channel: {}", config.messages_per_channel);
    println!("Batch size (for queue safety): {}", config.batch_size);

    let total_messages = config.channels * config.messages_per_channel;

    let barrier = Arc::new(Barrier::new(config.channels * 2 + 1));
    let mut pub_handles = vec![];
    let mut sub_handles = vec![];

    let error_count = Arc::new(Mutex::new(0));

    // Setup all channels
    for c in 0..config.channels {
        let host = config.host.clone();
        let port = config.port;
        let topic = format!("bench/topic/{}", c);
        let msgs = config.messages_per_channel;
        let batch_size = config.batch_size;
        let barrier = barrier.clone();
        let error_count = error_count.clone();

        let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);

        // Subscriber Thread
        let host_sub = host.clone();
        let topic_sub = topic.clone();
        let barrier_sub = barrier.clone();
        let err_sub = error_count.clone();
        sub_handles.push(thread::spawn(move || {
            let client_id = format!("bench-sub-{}", c);
            let mut stream = match connect_to_broker(&host_sub, port, &client_id) {
                Ok(s) => s,
                Err(_) => {
                    *err_sub.lock().unwrap() += 1;
                    return;
                }
            };

            // Subscribe
            let sub_pkt = encode_subscribe(1, &topic_sub);
            stream.write_all(&sub_pkt).unwrap();
            let (pt, _, _) = read_packet(&mut stream).unwrap();
            assert_eq!(pt, PT_SUBACK);

            barrier_sub.wait(); // Ready

            let mut received = 0;
            while received < msgs {
                let to_receive = std::cmp::min(batch_size, msgs - received);
                for _ in 0..to_receive {
                    let (pt, _, _) = read_packet(&mut stream).unwrap();
                    if pt == PT_PUBLISH {
                        received += 1;
                    } else {
                        // ignore pingresp etc
                    }
                }
                // Signal Publisher that a batch is done to prevent broker queue overflow
                let _ = tx.send(());
            }

            stream.write_all(&encode_disconnect()).unwrap();
        }));

        // Publisher Thread
        let host_pub = host.clone();
        let topic_pub = topic.clone();
        let barrier_pub = barrier.clone();
        let err_pub = error_count.clone();
        pub_handles.push(thread::spawn(move || {
            let client_id = format!("bench-pub-{}", c);
            let mut stream = match connect_to_broker(&host_pub, port, &client_id) {
                Ok(s) => s,
                Err(_) => {
                    *err_pub.lock().unwrap() += 1;
                    return None;
                }
            };

            barrier_pub.wait(); // Ready

            Some((stream, topic_pub, msgs, batch_size, rx))
        }));
    }

    // Wait for all connections to establish and subscriptions to settle
    barrier.wait();

    let start = Instant::now();

    // Now start the publishing loops
    let mut active_pubs = vec![];
    for handle in pub_handles.into_iter() {
        if let Some((mut stream, topic, msgs, batch_size, rx)) = handle.join().unwrap() {
            active_pubs.push(thread::spawn(move || {
                let payload = b"benchmark payload";
                let mut sent = 0;
                while sent < msgs {
                    let to_send = std::cmp::min(batch_size, msgs - sent);
                    for _ in 0..to_send {
                        stream.write_all(&encode_publish(&topic, payload)).unwrap();
                        sent += 1;
                    }
                    // Wait for sub to consume the batch
                    let _ = rx.recv();
                }
                stream.write_all(&encode_disconnect()).unwrap();
            }));
        }
    }

    for handle in active_pubs {
        let _ = handle.join();
    }

    // We should wait for subs to finish
    for handle in sub_handles {
        let _ = handle.join();
    }

    let duration = start.elapsed();
    let throughput = (total_messages as f64) / duration.as_secs_f64();

    let errs = *error_count.lock().unwrap();
    if errs > 0 {
        println!("WARNING: {} connection errors occurred.", errs);
    }

    println!("Total messages: {}", total_messages);
    println!("Time elapsed:   {:.2?}", duration);
    println!("Throughput:     {:.0} msg/s", throughput);
}
