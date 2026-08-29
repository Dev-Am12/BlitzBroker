//! Per-client connection handling: spawns a reader thread (socket ->
//! parsed packets -> `BrokerMessage`) and a writer thread (broker's
//! outbound queue -> encoded bytes -> socket) for each accepted TCP
//! connection.
//!
//! This is the integration point between Role A's broker.rs and Role B's
//! protocol.rs — see PLAN.md §8, the ~+8h checkpoint ("wire real parsing
//! into the broker"). Until protocol::decode/encode are implemented,
//! this module compiles but will panic at runtime on the first packet
//! (todo!() in protocol.rs) — that's expected, not a bug here.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::thread;

use crate::broker::{
    next_connection_id, BrokerMessage, ConnectionId, OutboundEvent,
    DEFAULT_CLIENT_QUEUE_CAPACITY,
};
use crate::protocol::{self, ConnAckPacket, ConnectReturnCode, MqttPacket};
use crate::queue;

/// Handle one accepted TCP connection for its entire lifetime. Blocks
/// until the client disconnects or a fatal protocol error occurs. Call
/// this on its own thread per connection (see main.rs's accept loop).
pub fn handle_connection(stream: TcpStream, broker_tx: Sender<BrokerMessage>) {
    let id: ConnectionId = next_connection_id();

    let write_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            crate::logging::warn(&format!("connection {id}: failed to clone socket: {e}"));
            return;
        }
    };

    let outbound = queue::new(DEFAULT_CLIENT_QUEUE_CAPACITY);
    let outbound_for_writer = outbound.clone();

    let writer = thread::spawn(move || {
        writer_loop(write_stream, outbound_for_writer, id);
    });

    // Reader runs on the calling thread; once it returns (disconnect or
    // fatal error), the outbound queue is closed so the writer thread
    // wakes up and exits too.
    reader_loop(stream, id, broker_tx, outbound);

    let _ = writer.join();
}

fn reader_loop(
    mut stream: TcpStream,
    id: ConnectionId,
    broker_tx: Sender<BrokerMessage>,
    outbound: queue::QueueHandle<OutboundEvent>,
) {
    let mut buf: Vec<u8> = Vec::new();
    let mut read_chunk = [0u8; 4096];

    loop {
        let n = match stream.read(&mut read_chunk) {
            Ok(0) => break, // client closed the connection
            Ok(n) => n,
            Err(e) => {
                crate::logging::warn(&format!("connection {id}: read error: {e}"));
                break;
            }
        };
        buf.extend_from_slice(&read_chunk[..n]);

        // Drain as many complete packets as are currently buffered.
        loop {
            match protocol::decode(&buf) {
                Ok((packet, consumed)) => {
                    buf.drain(..consumed);
                    if !dispatch_packet(id, packet, &broker_tx, &outbound) {
                        // Fatal per protocol (DISCONNECT received).
                        let _ = broker_tx.send(BrokerMessage::Disconnect { id });
                        outbound.close();
                        return;
                    }
                }
                Err(crate::error::ProtocolError::Incomplete) => break, // wait for more bytes
                Err(e) => {
                    crate::logging::warn(&format!("connection {id}: protocol error: {e}"));
                    let _ = broker_tx.send(BrokerMessage::Disconnect { id });
                    outbound.close();
                    return;
                }
            }
        }
    }

    let _ = broker_tx.send(BrokerMessage::Disconnect { id });
    outbound.close();
}

/// Handle one decoded packet. Returns `false` if the connection should
/// be torn down immediately after this (DISCONNECT received).
fn dispatch_packet(
    id: ConnectionId,
    packet: MqttPacket,
    broker_tx: &Sender<BrokerMessage>,
    outbound: &queue::QueueHandle<OutboundEvent>,
) -> bool {
    match packet {
        MqttPacket::Connect(connect) => {
            let _ = broker_tx.send(BrokerMessage::Register {
                id,
                client_id: connect.client_id,
                outbound: outbound.clone(),
            });
            // Core scope has no auth/session checks, so CONNACK is
            // always Accepted here. If auth is ever added (out of core
            // scope — see PLAN.md), this is where a rejection would be
            // returned instead of Register being sent.
            outbound.push(OutboundEvent::Packet(MqttPacket::ConnAck(ConnAckPacket {
                session_present: false,
                return_code: ConnectReturnCode::Accepted,
            })));
            true
        }
        MqttPacket::Subscribe(sub) => {
            // TODO (Role A/B, ~+8h-+20h checkpoint): send a real SUBACK
            // back with one return code per subscription, using
            // sub.packet_id. Left as a clear extension point rather than
            // guessed at here, since it needs packet_id plumbed through.
            for (topic, _qos) in sub.subscriptions {
                let _ = broker_tx.send(BrokerMessage::Subscribe { id, topic });
            }
            true
        }
        MqttPacket::Unsubscribe(unsub) => {
            // TODO: same as SUBACK above, for UNSUBACK.
            for topic in unsub.topic_filters {
                let _ = broker_tx.send(BrokerMessage::Unsubscribe { id, topic });
            }
            true
        }
        MqttPacket::Publish(publish) => {
            let _ = broker_tx.send(BrokerMessage::Publish {
                from: id,
                packet: publish,
            });
            true
        }
        MqttPacket::PingReq => {
            // Ping/pong is connection-local — no need to involve the
            // broker thread for it.
            outbound.push(OutboundEvent::Packet(MqttPacket::PingResp));
            true
        }
        MqttPacket::Disconnect => false,
        MqttPacket::ConnAck(_)
        | MqttPacket::SubAck(_)
        | MqttPacket::UnsubAck(_)
        | MqttPacket::PingResp => {
            // These are broker->client packets; a well-behaved client
            // should never send them to us. Ignore rather than tear
            // down the connection over it.
            true
        }
    }
}

fn writer_loop(mut stream: TcpStream, outbound: queue::QueueHandle<OutboundEvent>, id: ConnectionId) {
    while let Some(event) = outbound.pop_blocking() {
        let packet = match event {
            OutboundEvent::Packet(p) => p,
        };
        let bytes = protocol::encode(&packet);
        if let Err(e) = stream.write_all(&bytes) {
            crate::logging::warn(&format!("connection {id}: write error: {e}"));
            break;
        }
    }
}
