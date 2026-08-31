//! blitzclient — a minimal MQTT 3.1.1 pub/sub client for BlitzBroker.
//!
//! Usage:
//!   blitzclient --host <addr> --port <port> pub
//!               --topic <topic> --message <msg> [--qos 0|1]
//!
//!   blitzclient --host <addr> --port <port> sub --topic <filter>
//!
//! Implements only the packets needed for its two operation modes:
//!   pub:  CONNECT → CONNACK → PUBLISH → [PUBACK if QoS 1] → DISCONNECT
//!   sub:  CONNECT → CONNACK → SUBSCRIBE → SUBACK → loop(PUBLISH…)
//!
//! Self-contained: does not import from the broker's protocol.rs — see
//! DECISIONS.md #11. Each encode/decode function cites its spec section.

use std::io::{Read, Write};
use std::net::TcpStream;

// ─── Constants ────────────────────────────────────────────────────────────────

/// MQTT 3.1.1 §2.2.1 Table 2.1 — packet type nibble values.
const PT_CONNECT: u8 = 1;
const PT_CONNACK: u8 = 2;
const PT_PUBLISH: u8 = 3;
const PT_PUBACK: u8 = 4;
const PT_SUBSCRIBE: u8 = 8;
const PT_SUBACK: u8 = 9;
const PT_DISCONNECT: u8 = 14;

// ─── Wire-format helpers ───────────────────────────────────────────────────────

/// Encode a value as the MQTT "Remaining Length" variable-length field (§2.2.3).
/// Max value 268,435,455 (4 bytes). Algorithm from the spec Table 2.4.
fn encode_remaining_length(mut len: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(4);
    loop {
        let mut encoded_byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            encoded_byte |= 0x80; // set continuation bit
        }
        out.push(encoded_byte);
        if len == 0 {
            break;
        }
    }
    out
}

/// Decode the MQTT "Remaining Length" from the start of `buf`.
/// Returns `(value, bytes_consumed)` or an error string on failure.
/// Never panics.
fn decode_remaining_length(buf: &[u8]) -> Result<(u32, usize), String> {
    let mut value: u32 = 0;
    let mut i: usize = 0;
    loop {
        if i >= 4 {
            // §2.2.3: at most 4 bytes in this field.
            return Err("remaining length exceeds 4 bytes (§2.2.3)".into());
        }
        if i >= buf.len() {
            return Err("truncated remaining length field".into());
        }
        let byte = buf[i];
        // multiplier = 128^i, safe within u32 for i in 0..=3.
        let multiplier: u32 = 128u32.pow(i as u32);
        value += (byte as u32 & 0x7F) * multiplier;
        i += 1;
        if byte & 0x80 == 0 {
            break;
        }
    }
    Ok((value, i))
}

/// Encode a UTF-8 string as MQTT §1.5.3 length-prefixed format.
fn encode_utf8_str(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    let len = bytes.len().min(u16::MAX as usize) as u16;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&bytes[..len as usize]);
}

/// Decode a UTF-8 string from `buf` at position `pos` (§1.5.3).
/// Returns `(string, new_pos)` or an error string. Never panics.
fn decode_utf8_str(buf: &[u8], pos: usize) -> Result<(String, usize), String> {
    if pos + 2 > buf.len() {
        return Err(format!(
            "UTF-8 string length prefix truncated at pos {pos} (buf len {})",
            buf.len()
        ));
    }
    let str_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
    let start = pos + 2;
    let end = start
        .checked_add(str_len)
        .ok_or_else(|| "UTF-8 string length overflow".to_string())?;
    if end > buf.len() {
        return Err(format!(
            "UTF-8 string body truncated: need {end} bytes, buf has {}",
            buf.len()
        ));
    }
    let s = std::str::from_utf8(&buf[start..end])
        .map_err(|_| format!("invalid UTF-8 bytes at pos {start}..{end} (§1.5.3)"))?
        .to_string();
    Ok((s, end))
}

/// Build a complete packet: fixed header byte, remaining-length field, body.
fn make_packet(packet_type: u8, flags: u8, body: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push((packet_type << 4) | (flags & 0x0F));
    out.extend_from_slice(&encode_remaining_length(body.len() as u32));
    out.extend_from_slice(&body);
    out
}

// ─── Packet encoders ──────────────────────────────────────────────────────────

/// Encode CONNECT (§3.1).
///
/// Wire layout (body, after fixed header + remaining length):
///   Protocol Name (UTF-8 "MQTT") [6 bytes]
///   Protocol Level = 4           [1 byte]
///   Connect Flags = 0x02 (clean_session=1, no will/user/pass) [1 byte]
///   Keep Alive (big-endian u16)   [2 bytes]
///   Client Identifier (UTF-8)    [2 + len bytes]
fn encode_connect(client_id: &str, keep_alive: u16) -> Vec<u8> {
    let mut body = Vec::new();
    encode_utf8_str("MQTT", &mut body);       // §3.1.2.1
    body.push(4);                              // §3.1.2.2: protocol level 3.1.1
    body.push(0x02);                           // §3.1.2.3: clean_session bit
    body.extend_from_slice(&keep_alive.to_be_bytes()); // §3.1.2.10
    encode_utf8_str(client_id, &mut body);    // §3.1.3.1
    make_packet(PT_CONNECT, 0, body)
}

/// Encode SUBSCRIBE (§3.8).
///
/// Wire layout (body):
///   Packet Identifier (big-endian u16)        [2 bytes]
///   For each filter:
///     Topic Filter (UTF-8)                    [2 + len bytes]
///     Requested QoS (0 for core scope)        [1 byte]
///
/// Fixed-header flags MUST be 0b0010 per §3.8.1.
fn encode_subscribe(packet_id: u16, filter: &str, qos: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes()); // §3.8.2
    encode_utf8_str(filter, &mut body);               // §3.8.3
    body.push(qos & 0x03);                            // §3.8.3: requested QoS
    make_packet(PT_SUBSCRIBE, 0b0010, body)
}

/// Encode PUBLISH (§3.3).
///
/// Fixed-header flags: DUP=0, QoS in bits [2:1], RETAIN=0.
/// Body: topic (UTF-8), [packet_id u16 big-endian if QoS>0], payload.
fn encode_publish(topic: &str, payload: &[u8], qos: u8, packet_id: Option<u16>) -> Vec<u8> {
    let mut body = Vec::new();
    encode_utf8_str(topic, &mut body);  // §3.3.2.1
    if qos > 0 {
        // §3.3.2.2: packet identifier present only for QoS > 0.
        let pid = packet_id.unwrap_or(1);
        body.extend_from_slice(&pid.to_be_bytes());
    }
    body.extend_from_slice(payload);    // §3.3.3
    // flags: bit3=DUP(0), bit2-1=QoS, bit0=RETAIN(0)
    let flags = (qos & 0x03) << 1;
    make_packet(PT_PUBLISH, flags, body)
}

/// Encode DISCONNECT (§3.14). Fixed header only, remaining length = 0.
fn encode_disconnect() -> Vec<u8> {
    vec![(PT_DISCONNECT << 4), 0x00]
}

// ─── Packet decoders ─────────────────────────────────────────────────────────

/// Read exactly one MQTT packet from the stream.
/// Returns `(packet_type_nibble, flags_nibble, body_bytes)`.
/// On any IO or parse error, returns Err with a description.
/// Does not panic.
fn read_packet(stream: &mut TcpStream) -> Result<(u8, u8, Vec<u8>), String> {
    // Read byte 0: fixed header.
    let mut header_byte = [0u8; 1];
    stream
        .read_exact(&mut header_byte)
        .map_err(|e| format!("read error (fixed header byte 0): {e}"))?;
    let packet_type = header_byte[0] >> 4;
    let flags = header_byte[0] & 0x0F;

    // Read remaining-length field (1–4 bytes, continuation bit §2.2.3).
    // Read byte-by-byte until no continuation bit.
    let mut rl_bytes = Vec::with_capacity(4);
    loop {
        let mut b = [0u8; 1];
        stream
            .read_exact(&mut b)
            .map_err(|e| format!("read error (remaining length): {e}"))?;
        rl_bytes.push(b[0]);
        if b[0] & 0x80 == 0 {
            break;
        }
        if rl_bytes.len() >= 4 {
            return Err("remaining length > 4 bytes (§2.2.3)".into());
        }
    }
    let (remaining_len, _) = decode_remaining_length(&rl_bytes)?;

    // Read body.
    let body_len = remaining_len as usize;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        stream
            .read_exact(&mut body)
            .map_err(|e| format!("read error (body, {body_len} bytes): {e}"))?;
    }

    Ok((packet_type, flags, body))
}

/// Decode a CONNACK body (2 bytes, §3.2.2).
/// Returns `(session_present, return_code)`.
fn decode_connack_body(body: &[u8]) -> Result<(bool, u8), String> {
    if body.len() < 2 {
        return Err(format!(
            "CONNACK body too short: got {} bytes, need 2 (§3.2.2)",
            body.len()
        ));
    }
    let session_present = body[0] & 0x01 != 0;
    let return_code = body[1];
    Ok((session_present, return_code))
}

/// Decode a SUBACK body (§3.9.3).
/// Returns `(packet_id, return_codes)`.
///
/// Wire layout: [pid_hi, pid_lo, rc0, rc1, ...]
/// Return codes: 0x00 = success QoS 0, 0x01 = QoS 1, 0x02 = QoS 2,
///               0x80 = failure.
fn decode_suback_body(body: &[u8]) -> Result<(u16, Vec<u8>), String> {
    if body.len() < 3 {
        // Minimum: 2-byte packet_id + at least 1 return code.
        return Err(format!(
            "SUBACK body too short: got {} bytes, need ≥3 (§3.9.3)",
            body.len()
        ));
    }
    let packet_id = u16::from_be_bytes([body[0], body[1]]);
    let return_codes = body[2..].to_vec();
    Ok((packet_id, return_codes))
}

/// Decode a PUBLISH body (broker→client direction, §3.3).
/// Returns `(topic, payload, qos, packet_id)`.
///
/// Wire layout (body, i.e. bytes after the fixed header):
///   Topic Name (UTF-8)                    [2 + len bytes]
///   Packet Identifier (big-endian u16)    [2 bytes, only if QoS > 0]
///   Payload                               [remaining bytes]
///
/// `flags` is the low nibble of the fixed-header byte:
///   bit3 = DUP, bits2-1 = QoS, bit0 = RETAIN
fn decode_publish_body(flags: u8, body: &[u8]) -> Result<(String, Vec<u8>, u8, Option<u16>), String> {
    let qos = (flags >> 1) & 0x03;

    // Decode topic name (§3.3.2.1).
    let (topic, mut pos) = decode_utf8_str(body, 0)?;

    // Decode packet identifier (§3.3.2.2): present only for QoS > 0.
    let packet_id = if qos > 0 {
        if pos + 2 > body.len() {
            return Err(format!(
                "PUBLISH body truncated: need packet_id at pos {pos} but buf len {}",
                body.len()
            ));
        }
        let pid = u16::from_be_bytes([body[pos], body[pos + 1]]);
        if pid == 0 {
            return Err("PUBLISH QoS 1 packet_id must be non-zero (§2.3.1)".into());
        }
        pos += 2;
        Some(pid)
    } else {
        None
    };

    // Everything remaining is the payload (§3.3.3). May be empty.
    let payload = body[pos..].to_vec();

    Ok((topic, payload, qos, packet_id))
}

/// Decode a PUBACK body (2 bytes, §3.4.2) — returns the packet identifier.
fn decode_puback_body(body: &[u8]) -> Result<u16, String> {
    if body.len() < 2 {
        return Err(format!(
            "PUBACK body too short: got {} bytes, need 2 (§3.4.2)",
            body.len()
        ));
    }
    Ok(u16::from_be_bytes([body[0], body[1]]))
}

// ─── Shared connection setup ─────────────────────────────────────────────────

/// Shared: TCP connect + CONNECT + CONNACK.
/// Returns the ready stream on success, Err with description otherwise.
/// Reused by both `run_pub` and `run_sub` — no code duplication.
fn connect_to_broker(host: &str, port: u16) -> Result<TcpStream, String> {
    let addr = format!("{host}:{port}");
    let mut stream =
        TcpStream::connect(&addr).map_err(|e| format!("TCP connect to {addr} failed: {e}"))?;
    eprintln!("blitzclient: connected to {addr}");

    // Client ID: "blitzclient-<pid>" — unique per process, no counter needed.
    let client_id = format!("blitzclient-{}", std::process::id());
    let connect_pkt = encode_connect(&client_id, 60);
    stream
        .write_all(&connect_pkt)
        .map_err(|e| format!("write CONNECT failed: {e}"))?;
    eprintln!("blitzclient: sent CONNECT (client_id={client_id})");

    let (pkt_type, _flags, body) = read_packet(&mut stream)?;
    if pkt_type != PT_CONNACK {
        return Err(format!(
            "expected CONNACK (type {PT_CONNACK}), got packet type {pkt_type}"
        ));
    }
    let (_session_present, return_code) = decode_connack_body(&body)?;
    if return_code != 0x00 {
        let reason = match return_code {
            1 => "unacceptable protocol version",
            2 => "identifier rejected",
            3 => "server unavailable",
            4 => "bad username or password",
            5 => "not authorized",
            _ => "unknown return code",
        };
        return Err(format!(
            "CONNACK refused: return_code=0x{return_code:02X} ({reason})"
        ));
    }
    eprintln!("blitzclient: CONNACK accepted");
    Ok(stream)
}

// ─── CLI parsing ──────────────────────────────────────────────────────────────

enum Subcommand {
    Pub {
        topic: String,
        message: String,
        qos: u8,
    },
    Sub {
        filter: String,
    },
}

struct Cli {
    host: String,
    port: u16,
    subcommand: Subcommand,
}

fn usage() -> ! {
    eprintln!("Usage:");
    eprintln!("  blitzclient --host <addr> --port <port> pub --topic <topic> --message <msg> [--qos 0|1]");
    eprintln!("  blitzclient --host <addr> --port <port> sub --topic <filter>");
    std::process::exit(1);
}

fn parse_args() -> Cli {
    let args: Vec<String> = std::env::args().collect();

    let mut host: Option<String> = None;
    let mut port: Option<u16> = None;
    // pub fields
    let mut pub_topic: Option<String> = None;
    let mut message: Option<String> = None;
    let mut qos: u8 = 0;
    // sub fields
    let mut sub_filter: Option<String> = None;

    let mut subcmd: Option<&str> = None;
    // We do two passes: first scan for the subcommand word to know which
    // required fields to enforce; then parse all flags regardless of order.

    // Single-pass: track subcommand as soon as we see "pub" or "sub" (not
    // preceded by "--"). After parsing, validate required fields per subcmd.
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "pub" if !args[i].starts_with('-') => {
                subcmd = Some("pub");
            }
            "sub" if !args[i].starts_with('-') => {
                subcmd = Some("sub");
            }
            "--host" => {
                i += 1;
                host = args.get(i).cloned();
            }
            "--port" => {
                i += 1;
                port = args.get(i).and_then(|s| s.parse::<u16>().ok());
            }
            "--topic" | "--filter" => {
                i += 1;
                // "--topic" serves both pub (topic name) and sub (filter).
                // We store into whichever slot matches the detected subcmd.
                // If subcmd not yet known, store into both and let validation
                // sort it out — subcommand may come after the flags.
                let val = args.get(i).cloned();
                pub_topic = val.clone();
                sub_filter = val;
            }
            "--message" | "--msg" => {
                i += 1;
                message = args.get(i).cloned();
            }
            "--qos" => {
                i += 1;
                qos = args.get(i).and_then(|s| s.parse::<u8>().ok()).unwrap_or(0);
                if qos > 1 {
                    eprintln!("blitzclient: --qos must be 0 or 1 (QoS 2 is out of scope)");
                    std::process::exit(1);
                }
            }
            other => {
                eprintln!("blitzclient: unknown argument '{}' (ignored)", other);
            }
        }
        i += 1;
    }

    let host = host.unwrap_or_else(|| {
        eprintln!("blitzclient: --host is required");
        usage()
    });
    let port = port.unwrap_or_else(|| {
        eprintln!("blitzclient: --port is required or invalid");
        usage()
    });

    let subcommand = match subcmd {
        Some("pub") => {
            let topic = pub_topic.unwrap_or_else(|| {
                eprintln!("blitzclient pub: --topic is required");
                usage()
            });
            let message = message.unwrap_or_else(|| {
                eprintln!("blitzclient pub: --message is required");
                usage()
            });
            Subcommand::Pub { topic, message, qos }
        }
        Some("sub") => {
            let filter = sub_filter.unwrap_or_else(|| {
                eprintln!("blitzclient sub: --topic <filter> is required");
                usage()
            });
            Subcommand::Sub { filter }
        }
        _ => {
            eprintln!("blitzclient: subcommand 'pub' or 'sub' is required");
            usage()
        }
    };

    Cli { host, port, subcommand }
}

// ─── pub flow ─────────────────────────────────────────────────────────────────

fn run_pub(host: &str, port: u16, topic: &str, message: &str, qos: u8) -> Result<(), String> {
    let mut stream = connect_to_broker(host, port)?;

    // Encode and send PUBLISH (§3.3).
    let packet_id: Option<u16> = if qos == 1 { Some(1) } else { None };
    let publish_pkt = encode_publish(topic, message.as_bytes(), qos, packet_id);
    stream
        .write_all(&publish_pkt)
        .map_err(|e| format!("write PUBLISH failed: {e}"))?;
    eprintln!(
        "blitzclient: sent PUBLISH topic='{topic}' payload='{message}' qos={qos}"
    );

    // QoS 1: wait for PUBACK (§3.4).
    if qos == 1 {
        let pid = packet_id.unwrap(); // always Some when qos==1
        let (pkt_type, _flags, body) = read_packet(&mut stream)?;
        if pkt_type != PT_PUBACK {
            return Err(format!(
                "expected PUBACK (type {PT_PUBACK}) for QoS 1, got type {pkt_type}"
            ));
        }
        let acked_pid = decode_puback_body(&body)?;
        if acked_pid != pid {
            return Err(format!(
                "PUBACK packet_id mismatch: sent {pid}, broker acked {acked_pid} (§3.4.2)"
            ));
        }
        eprintln!("blitzclient: received PUBACK (packet_id={acked_pid}) — QoS 1 confirmed");
    }

    // Send DISCONNECT (§3.14).
    stream
        .write_all(&encode_disconnect())
        .map_err(|e| format!("write DISCONNECT failed: {e}"))?;
    eprintln!("blitzclient: sent DISCONNECT — done");
    Ok(())
}

// ─── sub flow ─────────────────────────────────────────────────────────────────

fn run_sub(host: &str, port: u16, filter: &str) -> Result<(), String> {
    let mut stream = connect_to_broker(host, port)?;

    // Encode and send SUBSCRIBE (§3.8).
    // A fixed packet_id of 1 is fine — this client sends exactly one
    // SUBSCRIBE per run.
    const SUB_PACKET_ID: u16 = 1;
    let subscribe_pkt = encode_subscribe(SUB_PACKET_ID, filter, 0);
    stream
        .write_all(&subscribe_pkt)
        .map_err(|e| format!("write SUBSCRIBE failed: {e}"))?;
    eprintln!("blitzclient: sent SUBSCRIBE filter='{filter}'");

    // Decode SUBACK (§3.9).
    let (pkt_type, _flags, body) = read_packet(&mut stream)?;
    if pkt_type != PT_SUBACK {
        return Err(format!(
            "expected SUBACK (type {PT_SUBACK}), got packet type {pkt_type}"
        ));
    }
    let (suback_pid, return_codes) = decode_suback_body(&body)?;
    if suback_pid != SUB_PACKET_ID {
        return Err(format!(
            "SUBACK packet_id mismatch: sent {SUB_PACKET_ID}, got {suback_pid}"
        ));
    }
    // Log the return code for the single subscription we sent.
    // 0x80 = failure; 0x00–0x02 = granted at the indicated QoS.
    match return_codes.first() {
        Some(&0x80) => {
            return Err(format!(
                "SUBACK: broker refused subscription to '{filter}' (return code 0x80)"
            ));
        }
        Some(&rc) => {
            eprintln!("blitzclient: SUBACK — subscription granted (QoS {rc})");
        }
        None => {
            return Err("SUBACK: no return codes received (§3.9.3)".into());
        }
    }

    // ── Receive loop ─────────────────────────────────────────────────────
    // Block-read from the socket, decode packets.
    // PUBLISH → print "<topic>: <payload>" to stdout.
    // Any other packet type: log to stderr and continue — don't panic,
    // don't exit. Loop ends when the connection closes (read_exact returns
    // an error) or the process is killed (normal for a subscriber tool).
    eprintln!("blitzclient: listening for messages (Ctrl+C to quit)…");
    loop {
        match read_packet(&mut stream) {
            Err(e) => {
                // Connection closed or IO error — this is the normal exit
                // path when the broker disconnects or the process is killed.
                eprintln!("blitzclient: connection closed ({e})");
                return Ok(());
            }
            Ok((pkt_type, flags, body)) => match pkt_type {
                PT_PUBLISH => {
                    match decode_publish_body(flags, &body) {
                        Ok((topic, payload, _qos, _pid)) => {
                            // Print topic and payload to stdout — same
                            // format as mosquitto_sub -v.
                            let payload_str = String::from_utf8_lossy(&payload);
                            println!("{topic}: {payload_str}");
                        }
                        Err(e) => {
                            // Malformed PUBLISH — log and continue, don't
                            // crash. AI_GUARDRAILS.md rule 3.
                            eprintln!("blitzclient: malformed PUBLISH ignored: {e}");
                        }
                    }
                }
                other => {
                    // Unexpected packet type — log and keep going.
                    eprintln!(
                        "blitzclient: unexpected packet type {other} (ignored)"
                    );
                }
            },
        }
    }
}

// ─── Entry point ─────────────────────────────────────────────────────────────

fn main() {
    let cli = parse_args();
    let result = match cli.subcommand {
        Subcommand::Pub { topic, message, qos } => {
            run_pub(&cli.host, cli.port, &topic, &message, qos)
        }
        Subcommand::Sub { filter } => run_sub(&cli.host, cli.port, &filter),
    };
    if let Err(e) = result {
        eprintln!("blitzclient error: {e}");
        std::process::exit(1);
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Remaining-length encode/decode round-trips ─────────────────────────
    // Cross-checked against §2.2.3 Table 2.4 worked examples.

    #[test]
    fn remaining_length_single_byte_zero() {
        let enc = encode_remaining_length(0);
        assert_eq!(enc, vec![0x00]);
        let (val, n) = decode_remaining_length(&enc).unwrap();
        assert_eq!(val, 0);
        assert_eq!(n, 1);
    }

    #[test]
    fn remaining_length_127() {
        // §2.2.3 Table 2.4: 127 → [0x7F]
        let enc = encode_remaining_length(127);
        assert_eq!(enc, vec![0x7F]);
        let (val, n) = decode_remaining_length(&enc).unwrap();
        assert_eq!(val, 127);
        assert_eq!(n, 1);
    }

    #[test]
    fn remaining_length_128() {
        // §2.2.3 Table 2.4: 128 → [0x80, 0x01]
        let enc = encode_remaining_length(128);
        assert_eq!(enc, vec![0x80, 0x01]);
        let (val, n) = decode_remaining_length(&enc).unwrap();
        assert_eq!(val, 128);
        assert_eq!(n, 2);
    }

    #[test]
    fn remaining_length_16383() {
        // §2.2.3 Table 2.4: 16383 → [0xFF, 0x7F]
        let enc = encode_remaining_length(16383);
        assert_eq!(enc, vec![0xFF, 0x7F]);
        let (val, n) = decode_remaining_length(&enc).unwrap();
        assert_eq!(val, 16383);
        assert_eq!(n, 2);
    }

    #[test]
    fn remaining_length_max() {
        // §2.2.3 Table 2.4: 268,435,455 → [0xFF, 0xFF, 0xFF, 0x7F]
        let enc = encode_remaining_length(268_435_455);
        assert_eq!(enc, vec![0xFF, 0xFF, 0xFF, 0x7F]);
        let (val, n) = decode_remaining_length(&enc).unwrap();
        assert_eq!(val, 268_435_455);
        assert_eq!(n, 4);
    }

    #[test]
    fn remaining_length_rejects_five_continuation_bytes() {
        // Five bytes all with continuation bit set — must fail, not panic.
        let result = decode_remaining_length(&[0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
        assert!(result.is_err(), "5 continuation bytes must be rejected");
    }

    #[test]
    fn remaining_length_truncated() {
        // Buffer ends before continuation bit is cleared.
        let result = decode_remaining_length(&[0x80]);
        assert!(result.is_err(), "truncated remaining length must be rejected");
    }

    // ── CONNECT encoding ───────────────────────────────────────────────────
    // Cross-check: the broker's protocol.rs encode_connect produces the same
    // wire bytes for the same inputs, per §3.1.2 layout. We verify structure
    // manually against the spec rather than importing protocol.rs.
    //
    // Expected wire bytes for encode_connect("test-client", 60):
    //
    // Fixed header: 0x10 (type=1, flags=0), remaining_length
    // Body:
    //   Protocol name: 0x00 0x04 'M' 'Q' 'T' 'T'    [6]
    //   Protocol level: 0x04                           [1]
    //   Connect flags: 0x02 (clean_session)            [1]
    //   Keep alive: 0x00 0x3C (60)                     [2]
    //   Client ID: 0x00 0x0B + "test-client"           [2+11=13]
    //   Total body = 23
    //   Remaining length = 23 = 0x17
    // Full packet: [0x10, 0x17, 0x00, 0x04, 'M','Q','T','T', 0x04, 0x02,
    //               0x00, 0x3C, 0x00, 0x0B, <11 bytes>]

    #[test]
    fn connect_packet_fixed_header_and_body() {
        let pkt = encode_connect("test-client", 60);
        // Fixed header byte 0: 0x10 (CONNECT, flags=0)
        assert_eq!(pkt[0], 0x10, "CONNECT fixed header byte 0");
        // Remaining length: body is 23 bytes → single-byte 0x17
        assert_eq!(pkt[1], 23, "CONNECT remaining length");
        // Protocol name: [0x00, 0x04, M, Q, T, T]
        assert_eq!(&pkt[2..8], &[0x00, 0x04, b'M', b'Q', b'T', b'T']);
        // Protocol level: 4
        assert_eq!(pkt[8], 4);
        // Connect flags: 0x02 (clean_session only)
        assert_eq!(pkt[9], 0x02);
        // Keep-alive: 60 = 0x003C big-endian
        assert_eq!(&pkt[10..12], &[0x00, 0x3C]);
        // Client ID length: 11 big-endian
        assert_eq!(&pkt[12..14], &[0x00, 0x0B]);
        // Client ID: "test-client"
        assert_eq!(&pkt[14..], b"test-client");
    }

    #[test]
    fn connect_packet_total_length() {
        let pkt = encode_connect("abc", 0);
        // Body = 6 + 1 + 1 + 2 + (2+3) = 15; with 2-byte fixed header = 17
        assert_eq!(pkt.len(), 17);
    }

    // ── SUBSCRIBE encoding ─────────────────────────────────────────────────
    // Cross-check: protocol.rs encode_subscribe produces the same bytes for
    // the same inputs (§3.8 wire layout).
    //
    // Expected for encode_subscribe(42, "test/#", 0):
    //   Fixed header byte 0: 0x82 (type=8, flags=0b0010)
    //   Remaining length: 2 + (2+6) + 1 = 11 bytes → 0x0B
    //   Body:
    //     [0x00, 0x2A]              — packet_id = 42
    //     [0x00, 0x06]              — topic filter len = 6
    //     [t, e, s, t, /, #]       — "test/#"
    //     [0x00]                    — requested QoS = 0

    #[test]
    fn subscribe_fixed_header_flags_are_0b0010() {
        let pkt = encode_subscribe(1, "a/b", 0);
        // Fixed header: 0x82 = (8 << 4) | 0b0010
        assert_eq!(pkt[0], 0x82, "SUBSCRIBE fixed header must have flags=0b0010 (§3.8.1)");
    }

    #[test]
    fn subscribe_packet_structure() {
        let pkt = encode_subscribe(42, "test/#", 0);
        assert_eq!(pkt[0], 0x82); // type=8, flags=0b0010
        // Remaining length: 2 + 2 + 6 + 1 = 11
        assert_eq!(pkt[1], 11);
        // packet_id = 42 = 0x002A
        assert_eq!(&pkt[2..4], &[0x00, 0x2A]);
        // topic filter length = 6
        assert_eq!(&pkt[4..6], &[0x00, 0x06]);
        // topic filter = "test/#"
        assert_eq!(&pkt[6..12], b"test/#");
        // requested QoS = 0
        assert_eq!(pkt[12], 0x00);
    }

    #[test]
    fn subscribe_wildcard_plus() {
        let pkt = encode_subscribe(1, "sensors/+/temp", 0);
        // Remaining length: 2 + 2 + 14 + 1 = 19
        assert_eq!(pkt[1], 19);
        // filter = "sensors/+/temp" (14 bytes)
        assert_eq!(&pkt[4..6], &[0x00, 14]);
        assert_eq!(&pkt[6..20], b"sensors/+/temp");
        assert_eq!(pkt[20], 0x00); // QoS
    }

    // ── CONNACK decoding ───────────────────────────────────────────────────
    // Cross-check: protocol.rs encode_connack produces [0x20, 0x02, sp, rc].
    // Our decoder must accept exactly those bytes.

    #[test]
    fn decode_connack_accepted() {
        // [session_present=0, return_code=0] = success
        let body = [0x00u8, 0x00];
        let (sp, rc) = decode_connack_body(&body).unwrap();
        assert!(!sp);
        assert_eq!(rc, 0x00);
    }

    #[test]
    fn decode_connack_refused_server_unavailable() {
        let body = [0x00u8, 0x03];
        let (sp, rc) = decode_connack_body(&body).unwrap();
        assert!(!sp);
        assert_eq!(rc, 0x03);
    }

    #[test]
    fn decode_connack_session_present_bit() {
        let body = [0x01u8, 0x00];
        let (sp, rc) = decode_connack_body(&body).unwrap();
        assert!(sp);
        assert_eq!(rc, 0x00);
    }

    #[test]
    fn decode_connack_body_too_short_does_not_panic() {
        assert!(decode_connack_body(&[]).is_err());
        assert!(decode_connack_body(&[0x00]).is_err());
    }

    // ── SUBACK decoding ────────────────────────────────────────────────────
    // Cross-check: protocol.rs encode_suback produces
    //   [0x90, remaining_len, pid_hi, pid_lo, rc0, rc1, ...]
    // Our decoder reads the body (the bytes after the fixed header),
    // i.e. [pid_hi, pid_lo, rc0, rc1, ...].

    #[test]
    fn decode_suback_single_subscription_granted() {
        // body = [pid_hi, pid_lo, return_code]
        let body = [0x00u8, 0x01, 0x00]; // packet_id=1, QoS 0 granted
        let (pid, rcs) = decode_suback_body(&body).unwrap();
        assert_eq!(pid, 1);
        assert_eq!(rcs, vec![0x00]);
    }

    #[test]
    fn decode_suback_failure_return_code() {
        // Return code 0x80 = failure (§3.9.3)
        let body = [0x00u8, 0x05, 0x80];
        let (pid, rcs) = decode_suback_body(&body).unwrap();
        assert_eq!(pid, 5);
        assert_eq!(rcs, vec![0x80]);
    }

    #[test]
    fn decode_suback_multiple_return_codes() {
        // A SUBACK for 3 subscriptions (would be sent for a multi-filter
        // SUBSCRIBE, e.g. from protocol.rs subscribe_roundtrip_multiple_topics).
        let body = [0x00u8, 0xFF, 0x00, 0x01, 0x80]; // pid=255, [0, 1, 0x80]
        let (pid, rcs) = decode_suback_body(&body).unwrap();
        assert_eq!(pid, 255);
        assert_eq!(rcs, vec![0x00, 0x01, 0x80]);
    }

    #[test]
    fn decode_suback_max_packet_id() {
        let body = [0xFF, 0xFF, 0x00]; // pid=65535, QoS 0
        let (pid, rcs) = decode_suback_body(&body).unwrap();
        assert_eq!(pid, 0xFFFF);
        assert_eq!(rcs, vec![0x00]);
    }

    #[test]
    fn decode_suback_too_short_does_not_panic() {
        assert!(decode_suback_body(&[]).is_err());
        assert!(decode_suback_body(&[0x00]).is_err());
        assert!(decode_suback_body(&[0x00, 0x01]).is_err()); // need ≥3 bytes
    }

    // ── PUBLISH decoding (broker→client) ──────────────────────────────────
    // Cross-check: our decode_publish_body must agree with what protocol.rs's
    // encode_publish produces for the same topic/payload.
    //
    // protocol.rs encode_publish body for topic="a/b", payload="hello", qos=0:
    //   [0x00, 0x03, 'a', '/', 'b', 'h', 'e', 'l', 'l', 'o']
    // flags nibble = 0 (QoS=0, DUP=0, RETAIN=0)

    #[test]
    fn decode_publish_body_qos0() {
        // Simulates broker sending: PUBLISH topic="a/b" payload="hello" QoS=0
        // Body (after fixed header): [0x00, 0x03, 'a', '/', 'b', payload...]
        let body = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x00, 0x03]); // topic len = 3
            b.extend_from_slice(b"a/b");
            b.extend_from_slice(b"hello");
            b
        };
        let flags = 0b0000u8; // QoS=0
        let (topic, payload, qos, pid) = decode_publish_body(flags, &body).unwrap();
        assert_eq!(topic, "a/b");
        assert_eq!(payload, b"hello");
        assert_eq!(qos, 0);
        assert_eq!(pid, None);
    }

    #[test]
    fn decode_publish_body_qos1_with_packet_id() {
        // QoS 1: body = [topic_len_hi, topic_len_lo, topic, pid_hi, pid_lo, payload]
        let body = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x00, 0x01]); // topic len = 1
            b.push(b't');
            b.extend_from_slice(&[0x00, 0x2A]); // packet_id = 42
            b.push(b'v');
            b
        };
        let flags = 0b0010u8; // QoS=1
        let (topic, payload, qos, pid) = decode_publish_body(flags, &body).unwrap();
        assert_eq!(topic, "t");
        assert_eq!(payload, b"v");
        assert_eq!(qos, 1);
        assert_eq!(pid, Some(42));
    }

    #[test]
    fn decode_publish_body_empty_payload() {
        // Empty payload is valid per §3.3.3.
        let body = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x00, 0x05]); // topic len = 5
            b.extend_from_slice(b"hello");
            // no payload bytes
            b
        };
        let flags = 0b0000u8;
        let (topic, payload, qos, pid) = decode_publish_body(flags, &body).unwrap();
        assert_eq!(topic, "hello");
        assert!(payload.is_empty());
        assert_eq!(qos, 0);
        assert_eq!(pid, None);
    }

    #[test]
    fn decode_publish_body_wildcard_topic_delivered() {
        // Broker delivers PUBLISH to "sensors/kitchen/temp" (matching "sensors/+/temp").
        // We just verify the decoder handles a three-level topic correctly.
        let topic_str = "sensors/kitchen/temp";
        let body = {
            let mut b = Vec::new();
            let tlen = topic_str.len() as u16;
            b.extend_from_slice(&tlen.to_be_bytes());
            b.extend_from_slice(topic_str.as_bytes());
            b.extend_from_slice(b"21C");
            b
        };
        let (topic, payload, qos, _) = decode_publish_body(0, &body).unwrap();
        assert_eq!(topic, "sensors/kitchen/temp");
        assert_eq!(payload, b"21C");
        assert_eq!(qos, 0);
    }

    #[test]
    fn decode_publish_body_truncated_topic_does_not_panic() {
        // Truncated body — must return Err, never panic.
        assert!(decode_publish_body(0, &[]).is_err());
        assert!(decode_publish_body(0, &[0x00, 0x05, b'h']).is_err()); // topic cut off
    }

    #[test]
    fn decode_publish_body_qos1_missing_packet_id_does_not_panic() {
        // flags say QoS=1 but body has no room for packet_id.
        let body = [0x00, 0x01, b't']; // topic "t", no pid bytes
        assert!(decode_publish_body(0b0010, &body).is_err());
    }

    // ── PUBLISH encoding ───────────────────────────────────────────────────
    // Verified against §3.3 wire layout.

    #[test]
    fn publish_qos0_no_packet_id() {
        let pkt = encode_publish("a/b", b"hello", 0, None);
        // Fixed header: 0x30 (PUBLISH, flags=0b0000)
        assert_eq!(pkt[0], 0x30);
        // Body: [0x00, 0x03, 'a', '/', 'b', 'h','e','l','l','o'] = 10 bytes
        // Remaining length = 10
        assert_eq!(pkt[1], 10);
        // Topic UTF-8 string
        assert_eq!(&pkt[2..4], &[0x00, 0x03]); // length=3
        assert_eq!(&pkt[4..7], b"a/b");
        // No packet ID for QoS 0
        assert_eq!(&pkt[7..], b"hello");
    }

    #[test]
    fn publish_qos1_has_packet_id() {
        let pkt = encode_publish("t", b"v", 1, Some(42));
        // Fixed header: 0x32 (PUBLISH, QoS=1, flags=0b0010)
        assert_eq!(pkt[0], 0x32);
        // Body: [0x00,0x01,'t', 0x00,0x2A, 'v'] = 6 bytes
        assert_eq!(pkt[1], 6);
        assert_eq!(&pkt[2..4], &[0x00, 0x01]); // topic len=1
        assert_eq!(pkt[4], b't');
        // packet_id = 42 = 0x002A big-endian
        assert_eq!(&pkt[5..7], &[0x00, 0x2A]);
        assert_eq!(pkt[7], b'v');
    }

    #[test]
    fn publish_empty_payload() {
        let pkt = encode_publish("x", b"", 0, None);
        // Body: [0x00, 0x01, 'x'] = 3 bytes; remaining length = 3
        assert_eq!(pkt[1], 3);
        assert_eq!(pkt.len(), 2 + 3);
    }

    // ── PUBACK decoding ────────────────────────────────────────────────────
    // Cross-check: protocol.rs encode_puback produces [0x40, 0x02, hi, lo].
    // Our decoder reads the body (after fixed header), which is [hi, lo].

    #[test]
    fn decode_puback_packet_id() {
        let body = [0x00u8, 0x2A]; // packet_id = 42
        let pid = decode_puback_body(&body).unwrap();
        assert_eq!(pid, 42);
    }

    #[test]
    fn decode_puback_max_packet_id() {
        let body = [0xFF, 0xFF]; // packet_id = 65535
        let pid = decode_puback_body(&body).unwrap();
        assert_eq!(pid, 0xFFFF);
    }

    #[test]
    fn decode_puback_body_too_short_does_not_panic() {
        assert!(decode_puback_body(&[]).is_err());
        assert!(decode_puback_body(&[0x00]).is_err());
    }

    // ── DISCONNECT ────────────────────────────────────────────────────────
    #[test]
    fn disconnect_is_two_bytes() {
        let pkt = encode_disconnect();
        assert_eq!(pkt, vec![0xE0, 0x00]);
    }

    // ── encode/decode round-trip ──────────────────────────────────────────
    // Verify that our encode_publish output is correctly decoded by
    // decode_publish_body — both directions from the same codebase.

    #[test]
    fn publish_encode_decode_roundtrip_qos0() {
        let encoded = encode_publish("home/sensors/temp", b"22.5C", 0, None);
        // Strip the 2-byte fixed header (byte0 + remaining_len) to get the body.
        // For this small packet remaining_len fits in 1 byte, so body starts at [2].
        let flags = (encoded[0] & 0x0F) as u8;
        let body = &encoded[2..];
        let (topic, payload, qos, pid) = decode_publish_body(flags, body).unwrap();
        assert_eq!(topic, "home/sensors/temp");
        assert_eq!(payload, b"22.5C");
        assert_eq!(qos, 0);
        assert_eq!(pid, None);
    }

    #[test]
    fn publish_encode_decode_roundtrip_qos1() {
        let encoded = encode_publish("alerts/door", b"open", 1, Some(7));
        let flags = (encoded[0] & 0x0F) as u8;
        let body = &encoded[2..];
        let (topic, payload, qos, pid) = decode_publish_body(flags, body).unwrap();
        assert_eq!(topic, "alerts/door");
        assert_eq!(payload, b"open");
        assert_eq!(qos, 1);
        assert_eq!(pid, Some(7));
    }
}
