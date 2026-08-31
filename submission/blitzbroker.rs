// BlitzBroker — single-file submission for the Zero Dependency hackathon (Track C).
// Build: rustc --edition 2021 -O -o blitzbroker blitzbroker.rs
// Zero third-party runtime dependencies. See STDLIB.md for substitutions.
//
// The broker implementation and its tests are deliberately top-level: this artifact
// contains no Rust module declarations, so a reader can follow it in one file.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
// Shared error types used across the broker.


/// Errors that can occur while decoding an MQTT packet from raw bytes.
///
/// Every variant here must correspond to a documented rejection path —
/// see AI_GUARDRAILS.md rule 3: parsers must never panic on untrusted
/// input, they must return one of these instead.
#[derive(Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// Not enough bytes have arrived yet to parse a full packet. Not fatal
    /// — connection.rs should keep buffering and retry once more bytes
    /// arrive.
    Incomplete,
    /// The "Remaining Length" field (MQTT 3.1.1 §2.2.3) was malformed
    /// (e.g. more than 4 continuation bytes).
    InvalidRemainingLength,
    /// The fixed header's packet type nibble did not match any known
    /// MQTT 3.1.1 control packet type (§2.2.1).
    UnknownPacketType(u8),
    /// A packet type or feature not in this broker's core subset
    /// (e.g. QoS 2 PUBLISH) — see PLAN.md §3/§4 for scope.
    UnsupportedFeature(&'static str),
    /// The payload for a given packet type did not match the structure
    /// required by the spec (wrong length, missing field, invalid UTF-8
    /// in a string field, etc).
    MalformedPayload(&'static str),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Incomplete => write!(f, "incomplete packet, need more bytes"),
            ProtocolError::InvalidRemainingLength => {
                write!(f, "invalid remaining length encoding")
            }
            ProtocolError::UnknownPacketType(t) => write!(f, "unknown packet type: {t:#04x}"),
            ProtocolError::UnsupportedFeature(s) => write!(f, "unsupported feature: {s}"),
            ProtocolError::MalformedPayload(s) => write!(f, "malformed payload: {s}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Errors surfaced by the broker actor or connection-handling layer.
#[derive(Debug)]
pub enum BrokerError {
    Io(std::io::Error),
    Protocol(ProtocolError),
}

impl From<std::io::Error> for BrokerError {
    fn from(e: std::io::Error) -> Self {
        BrokerError::Io(e)
    }
}

impl From<ProtocolError> for BrokerError {
    fn from(e: ProtocolError) -> Self {
        BrokerError::Protocol(e)
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrokerError::Io(e) => write!(f, "io error: {e}"),
            BrokerError::Protocol(e) => write!(f, "protocol error: {e}"),
        }
    }
}

impl std::error::Error for BrokerError {}

// ============================================================================

// A minimal hand-rolled leveled logger — see STDLIB.md (`log`/`tracing`
// substitution). Writes timestamped lines to stdout/stderr.
//
// Owner: Role D, but kept minimally functional here so Role A and
// connection.rs have something to call immediately. Extend freely
// (levels filtering, structured fields, etc.) without breaking this
// call signature.


fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn info(msg: &str) {
    println!("[{}] INFO  {msg}", timestamp());
}

pub fn warn(msg: &str) {
    println!("[{}] WARN  {msg}", timestamp());
}

pub fn error(msg: &str) {
    eprintln!("[{}] ERROR {msg}", timestamp());
}

// ============================================================================

// MQTT 3.1.1 wire protocol — packet types and (de)serialization.
//
// Owner: Role B. The enum/struct shapes below are the stable contract
// Role A's connection-handling code (connection.rs) is built against —
// treat them as fixed unless a change is logged in DECISIONS.md, since
// connection.rs depends on these exact names and fields. Fill in the
// `todo!()` bodies of `decode`/`encode`/the remaining-length functions.
//
// Spec: MQTT Version 3.1.1, OASIS Standard.
// Core scope (PLAN.md §3): CONNECT/CONNACK, SUBSCRIBE/SUBACK,
// UNSUBSCRIBE/UNSUBACK, PUBLISH (QoS 0 only), PINGREQ/PINGRESP,
// DISCONNECT. Extras (§4): wildcards, QoS 1, retained/will messages.


/// A fully decoded MQTT control packet.
#[derive(Debug, Clone)]
pub enum MqttPacket {
    Connect(ConnectPacket),
    ConnAck(ConnAckPacket),
    Subscribe(SubscribePacket),
    SubAck(SubAckPacket),
    Unsubscribe(UnsubscribePacket),
    UnsubAck(UnsubAckPacket),
    Publish(PublishPacket),
    PubAck(PubAckPacket),
    PingReq,
    PingResp,
    Disconnect,
}

/// MQTT 3.1.1 §3.1 CONNECT.
#[derive(Debug, Clone)]
pub struct ConnectPacket {
    pub protocol_name: String, // must be "MQTT"
    pub protocol_level: u8,    // must be 4 for 3.1.1
    pub clean_session: bool,
    pub keep_alive_secs: u16,
    pub client_id: String,
    // username/password/will fields: out of core scope — add if pursuing
    // the "last-will messages" extra (PLAN.md §4).
}

/// Connect return codes, MQTT 3.1.1 §3.2.2.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectReturnCode {
    Accepted = 0,
    UnacceptableProtocolVersion = 1,
    IdentifierRejected = 2,
    ServerUnavailable = 3,
    BadUsernameOrPassword = 4,
    NotAuthorized = 5,
}

/// MQTT 3.1.1 §3.2 CONNACK.
#[derive(Debug, Clone)]
pub struct ConnAckPacket {
    pub session_present: bool, // always false in core scope (no persistent sessions)
    pub return_code: ConnectReturnCode,
}

/// MQTT 3.1.1 §3.8 SUBSCRIBE.
#[derive(Debug, Clone)]
pub struct SubscribePacket {
    pub packet_id: u16,
    /// (topic filter, requested QoS) pairs. Core scope: QoS is always 0,
    /// but the field is still parsed per spec.
    pub subscriptions: Vec<(String, u8)>,
}

/// MQTT 3.1.1 §3.9 SUBACK.
#[derive(Debug, Clone)]
pub struct SubAckPacket {
    pub packet_id: u16,
    /// One return code per subscription in the corresponding SUBSCRIBE,
    /// same order. 0x00 = success at QoS 0, 0x80 = failure.
    pub return_codes: Vec<u8>,
}

/// MQTT 3.1.1 §3.10 UNSUBSCRIBE.
#[derive(Debug, Clone)]
pub struct UnsubscribePacket {
    pub packet_id: u16,
    pub topic_filters: Vec<String>,
}

/// MQTT 3.1.1 §3.11 UNSUBACK.
#[derive(Debug, Clone)]
pub struct UnsubAckPacket {
    pub packet_id: u16,
}

/// MQTT 3.1.1 §3.3 PUBLISH. Core scope: QoS 0 only (`qos` is always 0,
/// `packet_id` is always `None` — a QoS 0 PUBLISH has no packet
/// identifier per spec). Extra scope (PLAN.md §4 item 2): `qos == 1`
/// is also accepted, in which case `packet_id` is `Some`.
#[derive(Debug, Clone)]
pub struct PublishPacket {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub packet_id: Option<u16>,
}

/// MQTT 3.1.1 §3.4 PUBACK — extra scope (PLAN.md §4 item 2). Sent to
/// acknowledge receipt of a QoS 1 PUBLISH. Fixed format: just echoes
/// the Packet Identifier of the PUBLISH being acknowledged.
#[derive(Debug, Clone)]
pub struct PubAckPacket {
    pub packet_id: u16,
}

// --- Packet type nibble values, MQTT 3.1.1 §2.2.1 Table 2.1 ---
const PT_CONNECT: u8 = 1;
const PT_CONNACK: u8 = 2;
const PT_PUBLISH: u8 = 3;
const PT_PUBACK: u8 = 4;
const PT_SUBSCRIBE: u8 = 8;
const PT_SUBACK: u8 = 9;
const PT_UNSUBSCRIBE: u8 = 10;
const PT_UNSUBACK: u8 = 11;
const PT_PINGREQ: u8 = 12;
const PT_PINGRESP: u8 = 13;
const PT_DISCONNECT: u8 = 14;

/// Decode exactly one MQTT packet from the front of `buf`.
///
/// Returns `Ok((packet, bytes_consumed))` on success. Returns
/// `Err(ProtocolError::Incomplete)` if `buf` does not yet contain a full
/// packet — the caller (connection.rs) keeps buffering and retries, this
/// is not fatal. Any other `Err` means the input is genuinely malformed
/// and the connection should be closed.
///
/// MUST NOT panic on any input, including empty, truncated, or
/// adversarial byte sequences — AI_GUARDRAILS.md rule 3.
pub fn decode(buf: &[u8]) -> Result<(MqttPacket, usize), ProtocolError> {
    // Fixed header, §2.2: byte 0 = packet type (high nibble) + flags (low
    // nibble), followed by the variable-length Remaining Length field.
    if buf.is_empty() {
        return Err(ProtocolError::Incomplete);
    }
    let byte0 = buf[0];
    let packet_type = byte0 >> 4;
    let flags = byte0 & 0x0F;

    let (remaining_len, rl_bytes) = match decode_remaining_length(&buf[1..]) {
        Ok(v) => v,
        Err(ProtocolError::Incomplete) => return Err(ProtocolError::Incomplete),
        Err(e) => return Err(e),
    };

    let header_len = 1 + rl_bytes;
    let total_len = header_len
        .checked_add(remaining_len as usize)
        .ok_or(ProtocolError::MalformedPayload("remaining length overflow"))?;
    if buf.len() < total_len {
        return Err(ProtocolError::Incomplete);
    }
    let body = &buf[header_len..total_len];

    let packet = match packet_type {
        PT_CONNECT => MqttPacket::Connect(decode_connect(body)?),
        PT_SUBSCRIBE => MqttPacket::Subscribe(decode_subscribe(body)?),
        PT_UNSUBSCRIBE => MqttPacket::Unsubscribe(decode_unsubscribe(body)?),
        PT_PUBLISH => MqttPacket::Publish(decode_publish(flags, body)?),
        PT_PUBACK => {
            // Unlike SUBACK/CONNACK/UNSUBACK/PINGRESP, a client
            // legitimately *sends* PUBACK to the broker — it's
            // acknowledging a QoS 1 PUBLISH the broker delivered to
            // that client, so this direction is decoded, not rejected.
            if flags != 0 {
                return Err(ProtocolError::MalformedPayload(
                    "PUBACK fixed header flags are reserved and must be 0 (§3.4.1)",
                ));
            }
            MqttPacket::PubAck(decode_puback(body)?)
        }
        PT_PINGREQ => {
            if !body.is_empty() {
                return Err(ProtocolError::MalformedPayload(
                    "PINGREQ must have zero remaining length (§3.13.1)",
                ));
            }
            MqttPacket::PingReq
        }
        PT_DISCONNECT => {
            if !body.is_empty() {
                return Err(ProtocolError::MalformedPayload(
                    "DISCONNECT must have zero remaining length (§3.14.1)",
                ));
            }
            MqttPacket::Disconnect
        }
        // Broker -> client packets a well-behaved client should never
        // send us. Recognized (not UnknownPacketType) but rejected as
        // unsupported-from-a-client so the caller can decide how to log
        // it, rather than silently misparsing.
        PT_CONNACK => return Err(ProtocolError::UnsupportedFeature("CONNACK from client")),
        PT_SUBACK => return Err(ProtocolError::UnsupportedFeature("SUBACK from client")),
        PT_UNSUBACK => return Err(ProtocolError::UnsupportedFeature("UNSUBACK from client")),
        PT_PINGRESP => return Err(ProtocolError::UnsupportedFeature("PINGRESP from client")),
        other => return Err(ProtocolError::UnknownPacketType(other)),
    };

    Ok((packet, total_len))
}

/// Encode a packet to its wire representation.
pub fn encode(packet: &MqttPacket) -> Vec<u8> {
    match packet {
        MqttPacket::ConnAck(p) => encode_connack(p),
        MqttPacket::SubAck(p) => encode_suback(p),
        MqttPacket::UnsubAck(p) => encode_unsuback(p),
        MqttPacket::Publish(p) => encode_publish(p),
        MqttPacket::PubAck(p) => encode_puback(p),
        MqttPacket::PingResp => encode_fixed_header_only(PT_PINGRESP),
        MqttPacket::PingReq => encode_fixed_header_only(PT_PINGREQ),
        MqttPacket::Disconnect => encode_fixed_header_only(PT_DISCONNECT),
        // Client -> broker packets. The broker never sends these, but
        // encoding is still well-defined (useful for tests), so we
        // don't panic — just produce the correct bytes.
        MqttPacket::Connect(p) => encode_connect(p),
        MqttPacket::Subscribe(p) => encode_subscribe(p),
        MqttPacket::Unsubscribe(p) => encode_unsubscribe(p),
    }
}

/// Decode the MQTT "Remaining Length" variable-length field (§2.2.3)
/// starting at `buf`. Returns `(value, bytes_consumed)` on success.
/// Max 4 bytes, 7 data bits per byte, continuation bit in the MSB.
pub fn decode_remaining_length(buf: &[u8]) -> Result<(u32, usize), ProtocolError> {
    let mut value: u32 = 0;
    let mut i: usize = 0;

    loop {
        if i >= 4 {
            // §2.2.3: Remaining Length is at most 4 bytes. Checked
            // *before* touching a would-be 5th byte, so a run of 5+
            // continuation-bit-set bytes is rejected outright rather
            // than risking an overflow in the arithmetic below (a
            // multiplier for a 5th byte would exceed u32 range).
            return Err(ProtocolError::InvalidRemainingLength);
        }
        if i >= buf.len() {
            // Ran out of bytes before hitting a byte without the
            // continuation bit set — not necessarily malformed, just
            // not fully arrived yet.
            return Err(ProtocolError::Incomplete);
        }
        let encoded_byte = buf[i];
        // multiplier = 128^i; for i in 0..=3 this is at most 128^3 =
        // 2,097,152, so `(0x7F) * multiplier` and the running sum both
        // stay well within u32 range — no overflow possible here.
        let multiplier: u32 = 128u32.pow(i as u32);
        value += (encoded_byte as u32 & 0x7F) * multiplier;
        i += 1;

        if encoded_byte & 0x80 == 0 {
            break;
        }
    }

    Ok((value, i))
}

/// Encode a value as the MQTT "Remaining Length" variable-length field.
pub fn encode_remaining_length(len: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(4);
    let mut x = len;
    loop {
        let mut encoded_byte = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            encoded_byte |= 0x80;
        }
        out.push(encoded_byte);
        if x == 0 {
            break;
        }
    }
    out
}

// --- UTF-8 string field helpers, §1.5.3 ---
// Encoded as a 2-byte big-endian length prefix followed by the UTF-8
// bytes themselves (no null terminator).

fn decode_utf8_string(buf: &[u8]) -> Result<(String, usize), ProtocolError> {
    if buf.len() < 2 {
        return Err(ProtocolError::MalformedPayload(
            "truncated UTF-8 string length prefix (§1.5.3)",
        ));
    }
    let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let start: usize = 2;
    let end = start
        .checked_add(len)
        .ok_or(ProtocolError::MalformedPayload("UTF-8 string length overflow"))?;
    if buf.len() < end {
        return Err(ProtocolError::MalformedPayload(
            "UTF-8 string length prefix exceeds available bytes (§1.5.3)",
        ));
    }
    let s = std::str::from_utf8(&buf[start..end])
        .map_err(|_| ProtocolError::MalformedPayload("invalid UTF-8 in string field (§1.5.3)"))?
        .to_string();
    Ok((s, end))
}

fn encode_utf8_string(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    // Core scope doesn't need to handle >65535-byte strings gracefully
    // beyond not panicking; truncation here would itself be a spec
    // violation, so we saturate the length prefix rather than index
    // out of bounds. A string this large is already a caller bug.
    let len = bytes.len().min(u16::MAX as usize) as u16;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&bytes[..len as usize]);
}

fn encode_fixed_header_only(packet_type: u8) -> Vec<u8> {
    vec![packet_type << 4, 0x00]
}

fn prepend_fixed_header(packet_type: u8, flags: u8, mut body: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push((packet_type << 4) | (flags & 0x0F));
    out.extend_from_slice(&encode_remaining_length(body.len() as u32));
    out.append(&mut body);
    out
}

// --- CONNECT / CONNACK, §3.1 / §3.2 ---

fn decode_connect(body: &[u8]) -> Result<ConnectPacket, ProtocolError> {
    let (protocol_name, mut pos) = decode_utf8_string(body)?;
    if protocol_name != "MQTT" {
        return Err(ProtocolError::MalformedPayload(
            "CONNECT protocol name must be \"MQTT\" (§3.1.2.1)",
        ));
    }

    if body.len() < pos + 1 {
        return Err(ProtocolError::MalformedPayload("truncated CONNECT protocol level (§3.1.2.2)"));
    }
    let protocol_level = body[pos];
    if protocol_level != 4 {
        return Err(ProtocolError::UnsupportedFeature(
            "only MQTT protocol level 4 (3.1.1) is supported (§3.1.2.2)",
        ));
    }
    pos += 1;

    if body.len() < pos + 1 {
        return Err(ProtocolError::MalformedPayload("truncated CONNECT connect flags (§3.1.2.3)"));
    }
    let connect_flags = body[pos];
    let clean_session = connect_flags & 0x02 != 0;
    let will_flag = connect_flags & 0x04 != 0;
    let username_flag = connect_flags & 0x80 != 0;
    let password_flag = connect_flags & 0x40 != 0;
    pos += 1;

    if will_flag || username_flag || password_flag {
        // Will/username/password fields are out of core scope (see the
        // ConnectPacket doc comment) — reject rather than silently
        // dropping them, since silently dropping would misrepresent
        // what the client asked for.
        return Err(ProtocolError::UnsupportedFeature(
            "CONNECT with will/username/password flags is out of core scope (PLAN.md §3)",
        ));
    }

    if body.len() < pos + 2 {
        return Err(ProtocolError::MalformedPayload("truncated CONNECT keep alive (§3.1.2.10)"));
    }
    let keep_alive_secs = u16::from_be_bytes([body[pos], body[pos + 1]]);
    pos += 2;

    let (client_id, pos_after_id) = decode_utf8_string(&body[pos..])
        .map_err(|_| ProtocolError::MalformedPayload("truncated CONNECT client identifier (§3.1.3.1)"))?;
    pos += pos_after_id;

    if pos != body.len() {
        return Err(ProtocolError::MalformedPayload(
            "trailing bytes after CONNECT payload (will/username/password unsupported)",
        ));
    }

    Ok(ConnectPacket {
        protocol_name,
        protocol_level,
        clean_session,
        keep_alive_secs,
        client_id,
    })
}

fn encode_connect(p: &ConnectPacket) -> Vec<u8> {
    let mut body = Vec::new();
    encode_utf8_string(&p.protocol_name, &mut body);
    body.push(p.protocol_level);
    let mut flags = 0u8;
    if p.clean_session {
        flags |= 0x02;
    }
    body.push(flags);
    body.extend_from_slice(&p.keep_alive_secs.to_be_bytes());
    encode_utf8_string(&p.client_id, &mut body);
    prepend_fixed_header(PT_CONNECT, 0, body)
}

fn encode_connack(p: &ConnAckPacket) -> Vec<u8> {
    let mut body = Vec::with_capacity(2);
    body.push(if p.session_present { 0x01 } else { 0x00 });
    body.push(p.return_code as u8);
    prepend_fixed_header(PT_CONNACK, 0, body)
}

// --- Topic wildcards, §4.7 (extra scope: PLAN.md §4 item 1) ---
//
// A *topic filter* (used in SUBSCRIBE/UNSUBSCRIBE) may contain '+'
// (single-level) and '#' (multi-level) wildcards. A *topic name* (used
// in PUBLISH) must never contain either — already enforced in
// decode_publish above. See DECISIONS.md #9 for scope notes: this file
// validates filter syntax and provides the topic/filter matching
// predicate; wiring that predicate into the broker's fan-out logic
// (currently exact-match only, per broker.rs) is a Role A integration
// step, not done here.

/// Validate that a topic filter's wildcard usage is spec-legal.
/// Does not check UTF-8 validity (already guaranteed by
/// `decode_utf8_string`) or emptiness beyond §4.7.3's "at least one
/// character" rule.
pub fn validate_topic_filter(filter: &str) -> Result<(), ProtocolError> {
    if filter.is_empty() {
        return Err(ProtocolError::MalformedPayload(
            "topic filter must be at least one character (§4.7.3)",
        ));
    }
    let levels: Vec<&str> = filter.split('/').collect();
    let last_index = levels.len() - 1;
    for (i, level) in levels.iter().enumerate() {
        if level.contains('#') {
            if *level != "#" {
                return Err(ProtocolError::MalformedPayload(
                    "'#' must occupy an entire topic level on its own (§4.7.1.2)",
                ));
            }
            if i != last_index {
                return Err(ProtocolError::MalformedPayload(
                    "'#' must be the last level in a topic filter (§4.7.1.2)",
                ));
            }
        } else if level.contains('+') && *level != "+" {
            return Err(ProtocolError::MalformedPayload(
                "'+' must occupy an entire topic level on its own (§4.7.1.3)",
            ));
        }
    }
    Ok(())
}

/// Does `topic` (a concrete published topic name — never itself
/// containing wildcards) match `filter` (a subscription's topic
/// filter, which may contain `+`/`#`)? Per §4.7.1's matching rules.
///
/// Implemented iteratively rather than recursively: `topic`/`filter`
/// are attacker-influenced strings (arrive over the wire), and an
/// adversarial input with a huge number of `/` characters must not
/// risk a stack overflow — see AI_GUARDRAILS.md rule 3.
///
/// Callers should validate `filter` with `validate_topic_filter`
/// first; this function does not itself reject a malformed filter, it
/// just may not match anything.
pub fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    let topic_levels: Vec<&str> = topic.split('/').collect();
    let filter_levels: Vec<&str> = filter.split('/').collect();

    let mut ti = 0usize;
    let mut fi = 0usize;

    while fi < filter_levels.len() {
        let flevel = filter_levels[fi];
        if flevel == "#" {
            // '#' matches this level and everything below it,
            // including zero remaining topic levels (e.g.
            // "sport/#" matches the topic "sport" itself).
            return true;
        }
        if ti >= topic_levels.len() {
            // Filter has more (non-'#') levels than the topic has —
            // can't match.
            return false;
        }
        if flevel != "+" && flevel != topic_levels[ti] {
            return false;
        }
        ti += 1;
        fi += 1;
    }

    // Every filter level matched; only a match if the topic had no
    // leftover levels (a filter with no trailing '#' can't match a
    // longer topic).
    ti == topic_levels.len()
}

// --- SUBSCRIBE / SUBACK, §3.8 / §3.9 ---

fn decode_subscribe(body: &[u8]) -> Result<SubscribePacket, ProtocolError> {
    if body.len() < 2 {
        return Err(ProtocolError::MalformedPayload("truncated SUBSCRIBE packet identifier (§3.8.2)"));
    }
    let packet_id = u16::from_be_bytes([body[0], body[1]]);
    let mut pos = 2;

    if pos >= body.len() {
        return Err(ProtocolError::MalformedPayload(
            "SUBSCRIBE must contain at least one topic filter (§3.8.3)",
        ));
    }

    let mut subscriptions = Vec::new();
    while pos < body.len() {
        let (topic, consumed) = decode_utf8_string(&body[pos..])
            .map_err(|_| ProtocolError::MalformedPayload("truncated SUBSCRIBE topic filter (§3.8.3)"))?;
        pos += consumed;
        validate_topic_filter(&topic)?;
        if pos >= body.len() {
            return Err(ProtocolError::MalformedPayload(
                "SUBSCRIBE topic filter missing requested QoS byte (§3.8.3)",
            ));
        }
        let qos = body[pos];
        if qos > 2 {
            return Err(ProtocolError::MalformedPayload(
                "SUBSCRIBE requested QoS must be 0, 1, or 2 (§3.8.3)",
            ));
        }
        pos += 1;
        subscriptions.push((topic, qos));
    }

    Ok(SubscribePacket { packet_id, subscriptions })
}

fn encode_subscribe(p: &SubscribePacket) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&p.packet_id.to_be_bytes());
    for (topic, qos) in &p.subscriptions {
        encode_utf8_string(topic, &mut body);
        body.push(*qos);
    }
    // SUBSCRIBE's fixed header flags are reserved and MUST be 0010 per
    // §3.8.1.
    prepend_fixed_header(PT_SUBSCRIBE, 0b0010, body)
}

fn encode_suback(p: &SubAckPacket) -> Vec<u8> {
    let mut body = Vec::with_capacity(2 + p.return_codes.len());
    body.extend_from_slice(&p.packet_id.to_be_bytes());
    body.extend_from_slice(&p.return_codes);
    prepend_fixed_header(PT_SUBACK, 0, body)
}

// --- UNSUBSCRIBE / UNSUBACK, §3.10 / §3.11 ---

fn decode_unsubscribe(body: &[u8]) -> Result<UnsubscribePacket, ProtocolError> {
    if body.len() < 2 {
        return Err(ProtocolError::MalformedPayload("truncated UNSUBSCRIBE packet identifier (§3.10.2)"));
    }
    let packet_id = u16::from_be_bytes([body[0], body[1]]);
    let mut pos = 2;

    if pos >= body.len() {
        return Err(ProtocolError::MalformedPayload(
            "UNSUBSCRIBE must contain at least one topic filter (§3.10.3)",
        ));
    }

    let mut topic_filters = Vec::new();
    while pos < body.len() {
        let (topic, consumed) = decode_utf8_string(&body[pos..])
            .map_err(|_| ProtocolError::MalformedPayload("truncated UNSUBSCRIBE topic filter (§3.10.3)"))?;
        pos += consumed;
        validate_topic_filter(&topic)?;
        topic_filters.push(topic);
    }

    Ok(UnsubscribePacket { packet_id, topic_filters })
}

fn encode_unsubscribe(p: &UnsubscribePacket) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&p.packet_id.to_be_bytes());
    for topic in &p.topic_filters {
        encode_utf8_string(topic, &mut body);
    }
    // UNSUBSCRIBE's fixed header flags are reserved and MUST be 0010
    // per §3.10.1.
    prepend_fixed_header(PT_UNSUBSCRIBE, 0b0010, body)
}

fn encode_unsuback(p: &UnsubAckPacket) -> Vec<u8> {
    prepend_fixed_header(PT_UNSUBACK, 0, p.packet_id.to_be_bytes().to_vec())
}

// --- PUBLISH, §3.3 (QoS 0 core scope, QoS 1 extra scope PLAN.md §4 item 2) ---

fn decode_publish(flags: u8, body: &[u8]) -> Result<PublishPacket, ProtocolError> {
    let qos = (flags >> 1) & 0x03;
    let retain = flags & 0x01 != 0;

    if qos == 3 {
        return Err(ProtocolError::MalformedPayload(
            "PUBLISH QoS bits 11 is not a valid QoS value (§3.3.1.2)",
        ));
    }
    if qos == 2 {
        // QoS 2 is explicitly out of scope entirely (PLAN.md §4 item 2
        // only names QoS 1 as the extra).
        return Err(ProtocolError::UnsupportedFeature(
            "PUBLISH QoS 2 not supported (PLAN.md §4 scopes only QoS 0/1)",
        ));
    }

    let (topic, mut pos) = decode_utf8_string(body)
        .map_err(|_| ProtocolError::MalformedPayload("truncated PUBLISH topic name (§3.3.2.1)"))?;
    if topic.is_empty() {
        return Err(ProtocolError::MalformedPayload("PUBLISH topic name must not be empty (§4.7.3)"));
    }
    if topic.contains('+') || topic.contains('#') {
        // Wildcards are legal in a *subscription filter*, never in a
        // topic *name* being published to (§4.7.1).
        return Err(ProtocolError::MalformedPayload(
            "PUBLISH topic name must not contain wildcard characters (§4.7.1)",
        ));
    }

    // Packet Identifier is present only for QoS > 0 (§3.3.2.2), and per
    // §2.3.1 must be non-zero when present.
    let packet_id = if qos == 0 {
        None
    } else {
        if body.len() < pos + 2 {
            return Err(ProtocolError::MalformedPayload(
                "truncated PUBLISH packet identifier (§3.3.2.2)",
            ));
        }
        let id = u16::from_be_bytes([body[pos], body[pos + 1]]);
        pos += 2;
        if id == 0 {
            return Err(ProtocolError::MalformedPayload(
                "PUBLISH packet identifier must be non-zero for QoS > 0 (§2.3.1)",
            ));
        }
        Some(id)
    };

    let payload = body[pos..].to_vec();

    Ok(PublishPacket {
        topic,
        payload,
        qos,
        retain,
        packet_id,
    })
}

fn encode_publish(p: &PublishPacket) -> Vec<u8> {
    let mut body = Vec::new();
    encode_utf8_string(&p.topic, &mut body);
    if let Some(id) = p.packet_id {
        body.extend_from_slice(&id.to_be_bytes());
    }
    body.extend_from_slice(&p.payload);

    let mut flags = (p.qos & 0x03) << 1;
    if p.retain {
        flags |= 0x01;
    }
    // DUP flag intentionally always 0 on encode: this broker doesn't
    // implement redelivery/retry, only the ack round-trip itself (see
    // DECISIONS.md #10 for what "QoS 1" means in this codebase's
    // scope), so DUP never applies.
    prepend_fixed_header(PT_PUBLISH, flags, body)
}

// --- PUBACK, §3.4 (extra scope, PLAN.md §4 item 2) ---

fn decode_puback(body: &[u8]) -> Result<PubAckPacket, ProtocolError> {
    if body.len() != 2 {
        return Err(ProtocolError::MalformedPayload(
            "PUBACK variable header must be exactly a 2-byte packet identifier (§3.4.2)",
        ));
    }
    let packet_id = u16::from_be_bytes([body[0], body[1]]);
    if packet_id == 0 {
        return Err(ProtocolError::MalformedPayload(
            "PUBACK packet identifier must be non-zero (§2.3.1)",
        ));
    }
    Ok(PubAckPacket { packet_id })
}

fn encode_puback(p: &PubAckPacket) -> Vec<u8> {
    prepend_fixed_header(PT_PUBACK, 0, p.packet_id.to_be_bytes().to_vec())
}

// ============================================================================

// A small, bounded, drop-oldest message queue — the std-only substitute
// for what a crate like `crossbeam`'s bounded channel would normally
// give us. See STDLIB.md.
//
// `std::sync::mpsc::sync_channel(N)` was considered and rejected: it
// applies backpressure by *blocking the sender* when full, which is the
// wrong policy here — the broker thread must never stall because one
// subscriber is slow. We explicitly want to drop the oldest buffered
// message for that subscriber instead. See DECISIONS.md #4 and
// PLAN.md §3 (backpressure: bounded per-client outbound queue,
// drop-oldest).


struct BoundedDropOldestQueue<T> {
    items: Mutex<VecDeque<T>>,
    not_empty: Condvar,
    capacity: usize,
    closed: Mutex<bool>,
}

/// A cheaply-cloneable handle to a bounded drop-oldest queue, shared
/// between a producer (the broker thread, pushing outbound packets for
/// one client) and a consumer (that client's writer thread, draining and
/// writing to the socket).
pub struct QueueHandle<T> {
    queue: Arc<BoundedDropOldestQueue<T>>,
}

impl<T> Clone for QueueHandle<T> {
    fn clone(&self) -> Self {
        QueueHandle {
            queue: Arc::clone(&self.queue),
        }
    }
}

/// Create a new bounded drop-oldest queue with the given capacity.
pub fn new<T>(capacity: usize) -> QueueHandle<T> {
    let q = BoundedDropOldestQueue {
        items: Mutex::new(VecDeque::with_capacity(capacity)),
        not_empty: Condvar::new(),
        capacity,
        closed: Mutex::new(false),
    };
    QueueHandle { queue: Arc::new(q) }
}

impl<T> QueueHandle<T> {
    /// Push an item, dropping the oldest buffered item first if the
    /// queue is already at capacity. Returns `true` if an item was
    /// dropped to make room (callers may want to log this).
    pub fn push(&self, item: T) -> bool {
        let mut guard = self.queue.items.lock().unwrap();
        let dropped = if guard.len() >= self.queue.capacity {
            guard.pop_front();
            true
        } else {
            false
        };
        guard.push_back(item);
        drop(guard);
        self.queue.not_empty.notify_one();
        dropped
    }

    /// Block until an item is available (or the queue is closed and
    /// drained), then return it. Returns `None` once closed and empty.
    pub fn pop_blocking(&self) -> Option<T> {
        let mut guard = self.queue.items.lock().unwrap();
        loop {
            if let Some(item) = guard.pop_front() {
                return Some(item);
            }
            if *self.queue.closed.lock().unwrap() {
                return None;
            }
            guard = self.queue.not_empty.wait(guard).unwrap();
        }
    }

    /// Mark the queue closed and wake any blocked consumer so it can
    /// exit. Call this when the owning connection disconnects.
    pub fn close(&self) {
        *self.queue.closed.lock().unwrap() = true;
        self.queue.not_empty.notify_all();
    }
}

// ============================================================================

// Per-client connection handling: spawns a reader thread (socket ->
// parsed packets -> `BrokerMessage`) and a writer thread (broker's
// outbound queue -> encoded bytes -> socket) for each accepted TCP
// connection.


/// Abstraction over the broker channel so that `dispatch_packet` (and its
/// unit tests) can use a plain `Sender<BrokerMessage>` while the
/// production code path uses a `ShardedBroker`. Both implement this trait.
///
/// The trait is sealed to this module — nothing outside connection.rs
/// should need to implement it.
trait BrokerSend {
    fn broker_send(&self, msg: BrokerMessage);
}

impl BrokerSend for Sender<BrokerMessage> {
    fn broker_send(&self, msg: BrokerMessage) {
        // Ignore send errors: if the broker thread has exited, the
        // connection is about to be torn down anyway — the caller's
        // subsequent read error or disconnect handling will clean up.
        let _ = self.send(msg);
    }
}

impl BrokerSend for ShardedBroker {
    fn broker_send(&self, msg: BrokerMessage) {
        // Same policy: ignore errors on broker channel close.
        let _ = self.send(msg);
    }
}

/// Handle one accepted TCP connection for its entire lifetime. Blocks
/// until the client disconnects or a fatal protocol error occurs. Call
/// this on its own thread per connection (see main.rs's accept loop).
pub fn handle_connection(stream: TcpStream, broker: ShardedBroker) {
    let id: ConnectionId = next_connection_id();

    let write_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            warn(&format!("connection {id}: failed to clone socket: {e}"));
            return;
        }
    };

    let outbound = new(DEFAULT_CLIENT_QUEUE_CAPACITY);
    let outbound_for_writer = outbound.clone();

    let writer = thread::spawn(move || {
        writer_loop(write_stream, outbound_for_writer, id);
    });

    // Reader runs on the calling thread; once it returns (disconnect or
    // fatal error), the outbound queue is closed so the writer thread
    // wakes up and exits too.
    reader_loop(stream, id, broker, outbound);

    let _ = writer.join();
}

fn reader_loop(
    mut stream: TcpStream,
    id: ConnectionId,
    broker: ShardedBroker,
    outbound: QueueHandle<OutboundEvent>,
) {
    let mut buf: Vec<u8> = Vec::new();
    let mut read_chunk = [0u8; 4096];

    loop {
        let n = match stream.read(&mut read_chunk) {
            Ok(0) => break, // client closed the connection
            Ok(n) => n,
            Err(e) => {
                warn(&format!("connection {id}: read error: {e}"));
                break;
            }
        };
        buf.extend_from_slice(&read_chunk[..n]);

        // Drain as many complete packets as are currently buffered.
        loop {
            match decode(&buf) {
                Ok((packet, consumed)) => {
                    buf.drain(..consumed);
                    if !dispatch_packet(id, packet, &broker, &outbound) {
                        // Fatal per protocol (DISCONNECT received).
                        let _ = broker.send(BrokerMessage::Disconnect { id });
                        outbound.close();
                        return;
                    }
                }
                Err(ProtocolError::Incomplete) => break, // wait for more bytes
                Err(e) => {
                    warn(&format!("connection {id}: protocol error: {e}"));
                    let _ = broker.send(BrokerMessage::Disconnect { id });
                    outbound.close();
                    return;
                }
            }
        }
    }

    let _ = broker.send(BrokerMessage::Disconnect { id });
    outbound.close();
}

/// Handle one decoded packet. Returns `false` if the connection should
/// be torn down immediately after this (DISCONNECT received).
fn dispatch_packet(
    id: ConnectionId,
    packet: MqttPacket,
    broker_tx: &dyn BrokerSend,
    outbound: &QueueHandle<OutboundEvent>,
) -> bool {
    match packet {
        MqttPacket::Connect(connect) => {
            broker_tx.broker_send(BrokerMessage::Register {
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
            // MQTT 3.1.1 §3.9.3: SUBACK payload contains one return code
            // per topic filter in the SUBSCRIBE, in the same order.
            // Core scope: QoS 0 only, broker always accepts → 0x00.
            // Future rejection path: replace 0x00 with 0x80 for that
            // filter's slot without restructuring anything else here.
            let mut return_codes: Vec<u8> = Vec::with_capacity(sub.subscriptions.len());
            for (topic, _qos) in sub.subscriptions {
                broker_tx.broker_send(BrokerMessage::Subscribe { id, topic });
                // §3.9.3 Table 3.4: 0x00 = Success – Maximum QoS 0.
                return_codes.push(0x00);
            }
            outbound.push(OutboundEvent::Packet(MqttPacket::SubAck(SubAckPacket {
                packet_id: sub.packet_id,
                return_codes,
            })));
            true
        }
        MqttPacket::Unsubscribe(unsub) => {
            for topic in unsub.topic_filters {
                broker_tx.broker_send(BrokerMessage::Unsubscribe { id, topic });
            }
            // MQTT 3.1.1 §3.11: the broker MUST send UNSUBACK in response
            // to a UNSUBSCRIBE request (fixed-format, packet_id only).
            outbound.push(OutboundEvent::Packet(MqttPacket::UnsubAck(UnsubAckPacket {
                packet_id: unsub.packet_id,
            })));
            true
        }
        MqttPacket::Publish(publish) => {
            // MQTT 3.1.1 §3.3.4: when a client publishes at QoS 1, the
            // broker MUST respond with PUBACK acknowledging that
            // specific packet identifier — this was missing (Role A
            // caught it via a live mosquitto_pub -q 1 test hanging on
            // the ack). QoS 0 has no packet identifier and gets no
            // ack, per spec. This ack is sent immediately/synchronously
            // — it is not conditioned on the broker having found any
            // subscribers or having completed fan-out; PUBACK
            // acknowledges receipt by the broker, not delivery to
            // anyone downstream (§3.3.4 makes no such promise either).
            if publish.qos == 1 {
                if let Some(packet_id) = publish.packet_id {
                    outbound.push(OutboundEvent::Packet(MqttPacket::PubAck(PubAckPacket {
                        packet_id,
                    })));
                }
                // `publish.packet_id` is `None` here only if decode()
                // let a malformed QoS-1 PUBLISH through, which it
                // shouldn't (protocol.rs validates this) — silently
                // skipping the ack rather than panicking is the safe
                // fallback either way.
            }
            broker_tx.broker_send(BrokerMessage::Publish {
                from: id,
                packet: publish,
            });
            true
        }
        MqttPacket::PubAck(ack) => {
            // ROLE B ADDED THIS ARM — flagging for Role A review, not
            // claiming ownership of connection.rs. Needed because
            // adding MqttPacket::PubAck (PLAN.md §4 item 2 / QoS 1)
            // made the match here non-exhaustive, which broke the
            // build; a correct-but-minimal arm was required to keep
            // `cargo build` green rather than leaving it broken.
            //
            // What this does: a client sends PUBACK to ack a QoS 1
            // PUBLISH the broker delivered *to* it. There's currently
            // no per-subscriber "pending ack" bookkeeping in
            // broker.rs, so there's nothing to clear yet — this is
            // therefore a correct no-op, not a stub. Once/if broker.rs
            // tracks in-flight QoS 1 deliveries, this is the extension
            // point: forward `ack.packet_id` (and `id`, the connection
            // it came from) to the broker so it can clear that entry.
            let _ = ack;
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

fn writer_loop(mut stream: TcpStream, outbound: QueueHandle<OutboundEvent>, id: ConnectionId) {
    while let Some(event) = outbound.pop_blocking() {
        let packet = match event {
            OutboundEvent::Packet(p) => p,
        };
        let bytes = encode(&packet);
        if let Err(e) = stream.write_all(&bytes) {
            warn(&format!("connection {id}: write error: {e}"));
            break;
        }
    }
}

// ============================================================================

// The broker actor: owns the topic -> subscriber registry exclusively.
// All other threads communicate with it only via `BrokerMessage` over an
// `std::sync::mpsc` channel — see DECISIONS.md #1 for why (no data
// races on the registry, per-topic publish order preserved, both by
// construction, not by careful locking).
//
// Sharding (PLAN.md §4 item 3 / DECISIONS.md #1 upgrade path):
// `ShardedBroker` runs N independent `run_broker` threads, each owning a
// disjoint subset of topics. A topic deterministically maps to one shard
// via `shard_for_topic` (hash modulo N), so no cross-shard coordination is
// ever needed. Register/Disconnect are broadcast to all shards because
// every shard must know about every client (a client may later subscribe
// to a topic owned by any shard). This redundancy is intentional and
// cheap at this scale — see DECISIONS.md #1 for the full reasoning.
//
// Wildcard subscriptions (PLAN.md §4 item 1 / DECISIONS.md #5):
// A subscription filter containing '+' or '#' is broadcast to *all*
// shards (same broadcast rule as Register/Disconnect). This is necessary
// because a publish to "sensors/kitchen/temp" hashes to the shard for
// that literal string, not to the shard that received "sensors/+/temp".
// Every shard therefore holds every wildcard filter and checks them
// against incoming publishes via `topic_matches_filter`.
// Exact-match filters (no wildcards) continue to route to a single shard
// by hash, unchanged — the fast path is preserved.
//
// Retained messages (PLAN.md §4 item 4):
// Each shard owns a `retained: HashMap<String, PublishPacket>` alongside
// its subscriber registry. A `Publish` with `retain=true` stores or clears
// (on empty payload, §3.3.1.3) the retained message for that topic — on the
// same shard that receives the publish (which is always `shard_for_topic(T)`
// for concrete topic T). A `Subscribe` checks this store for matches and
// immediately delivers them to the new subscriber. Sharding is correct by
// construction: exact-match subscribes route to the same shard as the
// retained message; wildcard subscribes broadcast to ALL shards (DECISIONS
// #9), so the broadcast guarantees every retained message is reachable.
// No cross-shard coordination is required.
//
// Owner: Role A.


/// Return true if `filter` contains MQTT wildcard characters ('+' or '#').
/// Used to decide whether a Subscribe/Unsubscribe must be broadcast to
/// all shards (wildcard) or routed to a single shard by hash (exact match).
/// The filter's wildcard syntax is already validated upstream by
/// `validate_topic_filter` before the `BrokerMessage` is built.
#[inline]
fn is_wildcard_filter(filter: &str) -> bool {
    filter.contains('+') || filter.contains('#')
}

/// Outbound queue capacity per connected client. Tunable — see PLAN.md §3
/// (backpressure: bounded per-client outbound queue, drop-oldest).
pub const DEFAULT_CLIENT_QUEUE_CAPACITY: usize = 128;

/// Number of broker shards. Each shard is an independent `run_broker`
/// thread owning a disjoint topic subset — see PLAN.md §4 item 3.
/// Tunable like DEFAULT_CLIENT_QUEUE_CAPACITY; 4 is a reasonable default
/// for development/demo hardware and demonstrates the architecture.
pub const NUM_BROKER_SHARDS: usize = 4;

/// Determine which shard owns `topic`. Pure, deterministic: the same
/// (topic, num_shards) pair always maps to the same shard index, so
/// every Subscribe/Unsubscribe/Publish for a given topic always reaches
/// the same shard — no cross-shard coordination required.
///
/// Uses `std::collections::hash_map::DefaultHasher` (std-only, no crate).
/// The specific hash value is an implementation detail; only the mod-N
/// determinism guarantee is part of the public contract.
pub fn shard_for_topic(topic: &str, num_shards: usize) -> usize {
    // Guard: if somehow called with 0 shards, return 0 rather than
    // panicking on the % — shouldn't happen in practice since
    // spawn_sharded_broker requires num_shards >= 1.
    if num_shards == 0 {
        return 0;
    }
    let mut h = DefaultHasher::new();
    topic.hash(&mut h);
    (h.finish() as usize) % num_shards
}

/// Internal registry key for a connected client.
///
/// Deliberately NOT the MQTT `client_id` string from the CONNECT packet:
/// the spec allows a client to reconnect with the same ID, and handling
/// that correctly (evicting the old session) is out of core scope — see
/// PLAN.md §4. An internal counter sidesteps that problem entirely for
/// the core build. The MQTT-level client_id is still tracked as
/// metadata (see `ClientState`) for logging.
pub type ConnectionId = u64;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate a new unique connection id. Called once per accepted TCP
/// connection, in connection.rs.
pub fn next_connection_id() -> ConnectionId {
    NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
}

/// A message a connection thread sends to the broker actor thread. This
/// is the stable contract connection.rs is built against — treat as
/// fixed unless a change is logged in DECISIONS.md.
pub enum BrokerMessage {
    /// A client finished CONNECT and is ready to be tracked. The broker
    /// won't route anything to a client until it registers here.
    Register {
        id: ConnectionId,
        client_id: String,
        outbound: QueueHandle<OutboundEvent>,
    },
    Subscribe {
        id: ConnectionId,
        topic: String,
    },
    Unsubscribe {
        id: ConnectionId,
        topic: String,
    },
    Publish {
        from: ConnectionId,
        packet: PublishPacket,
    },
    Disconnect {
        id: ConnectionId,
    },
}

/// What the broker pushes onto a client's outbound queue. The writer
/// thread (connection.rs) turns these into wire bytes via
/// `encode`.
pub enum OutboundEvent {
    Packet(MqttPacket),
}

struct ClientState {
    /// Kept for logging only — the registry itself is keyed by
    /// `ConnectionId`, not this. See the `ConnectionId` doc comment.
    #[allow(dead_code)]
    client_id: String,
    outbound: QueueHandle<OutboundEvent>,
}

/// The broker actor's main loop. Run this on its own dedicated thread
/// (see main.rs) — it owns the registry exclusively for as long as it
/// runs; nothing else may touch topic/subscriber state directly.
pub fn run_broker(rx: Receiver<BrokerMessage>) {
    let mut clients: HashMap<ConnectionId, ClientState> = HashMap::new();
    // topic -> subscribed connection ids. Supports both exact filters and
    // wildcard filters ('+' / '#'); see DECISIONS.md #5/#9.
    let mut topics: HashMap<String, Vec<ConnectionId>> = HashMap::new();
    // Retained messages (PLAN.md §4 item 4 / §3.3.1.3):
    // Maps an exact topic string → the most recent retained PublishPacket
    // for that topic. Only concrete topic names appear as keys (PUBLISH
    // topic names never contain wildcards per §4.7.1). Access/mutation is
    // safe here: this map is local to this shard's thread, never shared.
    let mut retained: HashMap<String, PublishPacket> = HashMap::new();

    for msg in rx {
        match msg {
            BrokerMessage::Register {
                id,
                client_id,
                outbound,
            } => {
                clients.insert(id, ClientState { client_id, outbound });
            }
            BrokerMessage::Subscribe { id, topic } => {
                let subs = topics.entry(topic.clone()).or_default();
                if !subs.contains(&id) {
                    subs.push(id);
                }

                // ── Retained-message replay (PLAN.md §4 item 4, §3.3.1.3) ──
                // After registering, immediately deliver any retained message
                // whose topic matches this filter — exact comparison for plain
                // filters, topic_matches_filter for wildcards.
                //
                // The delivered packet has retain=true so the client can
                // distinguish a retained replay from a live message (§3.3.1.3).
                if let Some(client) = clients.get(&id) {
                    if is_wildcard_filter(&topic) {
                        // Wildcard: scan all retained topics for matches.
                        // Collect first to avoid borrowing `retained` mutably
                        // while also borrowing `clients`.
                        let matches: Vec<PublishPacket> = retained
                            .values()
                            .filter(|p| topic_matches_filter(&p.topic, &topic))
                            .cloned()
                            .collect();
                        for mut pkt in matches {
                            pkt.retain = true; // mark as retained replay
                            client.outbound.push(
                                OutboundEvent::Packet(MqttPacket::Publish(pkt))
                            );
                        }
                    } else {
                        // Exact match: O(1) lookup.
                        if let Some(pkt) = retained.get(&topic) {
                            let mut pkt = pkt.clone();
                            pkt.retain = true;
                            client.outbound.push(
                                OutboundEvent::Packet(MqttPacket::Publish(pkt))
                            );
                        }
                    }
                }
            }
            BrokerMessage::Unsubscribe { id, topic } => {
                if let Some(subs) = topics.get_mut(&topic) {
                    subs.retain(|&sid| sid != id);
                }
            }
            BrokerMessage::Publish { from: _, packet } => {
                // ── Retained-message store update (§3.3.1.3) ──────────────
                // Must happen before fan-out so that subscribers who connect
                // during this very message-loop iteration get the latest
                // retained value if they also send Subscribe in the same
                // burst — in practice the ordering is serial so either order
                // is correct, but storing first is more spec-natural.
                if packet.retain {
                    if packet.payload.is_empty() {
                        // §3.3.1.3: a retain=true PUBLISH with empty payload
                        // is a "delete" instruction — remove any stored
                        // retained message for this topic.
                        retained.remove(&packet.topic);
                    } else {
                        // Store (overwriting any previous retained message
                        // for this exact topic).
                        retained.insert(packet.topic.clone(), packet.clone());
                    }
                }

                // ── Exact-match fast path (unchanged) ──────────────────────
                // Keep a set of already-notified connection IDs so the wildcard
                // pass below never double-delivers to a subscriber that also
                // has an exact-match subscription for the same topic.
                let mut notified: Vec<ConnectionId> = Vec::new();

                if let Some(subs) = topics.get(&packet.topic) {
                    for &sid in subs {
                        if let Some(client) = clients.get(&sid) {
                            client
                                .outbound
                                .push(OutboundEvent::Packet(MqttPacket::Publish(packet.clone())));
                            notified.push(sid);
                        }
                    }
                }

                // ── Wildcard pass (PLAN.md §4 item 1 / DECISIONS.md #5) ────
                // Iterate only entries whose filter key contains '+' or '#';
                // skip the exact-match key we already handled above so we
                // don't double-check it.
                // `topic_matches_filter` is iterative (no recursion), so it
                // cannot stack-overflow on adversarial inputs — GUARDRAILS §3.
                for (filter, subs) in topics.iter() {
                    if !is_wildcard_filter(filter) {
                        continue; // exact-match filters already handled above
                    }
                    if !topic_matches_filter(&packet.topic, filter) {
                        continue;
                    }
                    for &sid in subs {
                        // Skip clients already notified via exact-match.
                        if notified.contains(&sid) {
                            continue;
                        }
                        if let Some(client) = clients.get(&sid) {
                            client
                                .outbound
                                .push(OutboundEvent::Packet(MqttPacket::Publish(packet.clone())));
                            notified.push(sid);
                        }
                    }
                }
            }
            BrokerMessage::Disconnect { id } => {
                clients.remove(&id);
                for subs in topics.values_mut() {
                    subs.retain(|&sid| sid != id);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sharded broker — PLAN.md §4 item 3
// ---------------------------------------------------------------------------

/// A cheaply-cloneable handle to a sharded broker. Each shard is an
/// independent `run_broker` thread; this handle routes messages to the
/// correct shard (for topic-keyed messages) or broadcasts to all shards
/// (for Register / Disconnect).
///
/// Implement `Clone` manually because `Arc<Vec<Sender<...>>>` is already
/// `Clone` — derive would work too but manual is explicit about cost (O(1)).
#[derive(Clone)]
pub struct ShardedBroker {
    /// One sender per shard, indexed 0..num_shards.
    shard_txs: Arc<Vec<Sender<BrokerMessage>>>,
}

impl ShardedBroker {
    /// Route `msg` to the appropriate shard(s):
    /// - Subscribe / Unsubscribe / Publish → single shard determined by
    ///   `shard_for_topic(topic, num_shards)` (topic field of the message).
    /// - Register / Disconnect → **all** shards, because each shard needs
    ///   to know about every client before that client might subscribe to
    ///   one of its topics, and on disconnect every shard must clean up
    ///   regardless of whether that client ever subscribed to a topic it
    ///   owns. The per-shard `clients` map therefore holds every connected
    ///   client, even those with zero subscriptions on that shard — this
    ///   redundancy is intentional and cheap at this scale.
    ///
    /// Returns `Err` only if all target shard channels have been closed
    /// (broker threads exited); callers should treat this the same as a
    /// disconnected single channel.
    pub fn send(&self, msg: BrokerMessage) -> Result<(), ()> {
        let n = self.shard_txs.len();
        // Determine routing strategy up front by inspecting the message
        // without consuming it. We extract a copy of relevant fields so
        // the subsequent owned match has no borrowed-field conflicts.
        enum Routing { SingleShard(usize), Broadcast }
        let routing = match &msg {
            BrokerMessage::Subscribe { topic, .. }
            | BrokerMessage::Unsubscribe { topic, .. } => {
                if is_wildcard_filter(topic) {
                    // Wildcard filters must be present on every shard so that
                    // each shard can check them against incoming publishes —
                    // see DECISIONS.md #5 and the module-level doc comment.
                    Routing::Broadcast
                } else {
                    Routing::SingleShard(shard_for_topic(topic, n))
                }
            }
            BrokerMessage::Publish { packet, .. } => {
                Routing::SingleShard(shard_for_topic(&packet.topic, n))
            }
            BrokerMessage::Register { .. } | BrokerMessage::Disconnect { .. } => {
                Routing::Broadcast
            }
        };

        match routing {
            Routing::SingleShard(shard) => {
                self.shard_txs[shard].send(msg).map_err(|_| ())
            }
            Routing::Broadcast => {
                // BrokerMessage is not Clone, so reconstruct per-shard copies
                // by decomposing the owned value.
                let mut any_ok = false;
                match msg {
                    BrokerMessage::Register { id, ref client_id, ref outbound } => {
                        for i in 0..n {
                            let m = BrokerMessage::Register {
                                id,
                                client_id: client_id.clone(),
                                outbound: outbound.clone(),
                            };
                            if self.shard_txs[i].send(m).is_ok() {
                                any_ok = true;
                            }
                        }
                    }
                    BrokerMessage::Disconnect { id } => {
                        for tx in self.shard_txs.iter() {
                            if tx.send(BrokerMessage::Disconnect { id }).is_ok() {
                                any_ok = true;
                            }
                        }
                    }
                    BrokerMessage::Subscribe { id, ref topic } => {
                        for tx in self.shard_txs.iter() {
                            let m = BrokerMessage::Subscribe {
                                id,
                                topic: topic.clone(),
                            };
                            if tx.send(m).is_ok() {
                                any_ok = true;
                            }
                        }
                    }
                    BrokerMessage::Unsubscribe { id, ref topic } => {
                        for tx in self.shard_txs.iter() {
                            let m = BrokerMessage::Unsubscribe {
                                id,
                                topic: topic.clone(),
                            };
                            if tx.send(m).is_ok() {
                                any_ok = true;
                            }
                        }
                    }
                    // Publish is always SingleShard — unreachable here.
                    BrokerMessage::Publish { .. } => unreachable!(),
                }
                if any_ok { Ok(()) } else { Err(()) }
            }
        }
    }
}

/// Spawn `num_shards` independent broker threads and return a
/// `ShardedBroker` that routes messages to them.
///
/// # Panics
/// Panics if `num_shards == 0` — at least one shard is required.
pub fn spawn_sharded_broker(num_shards: usize) -> ShardedBroker {
    assert!(num_shards >= 1, "num_shards must be at least 1");
    let mut shard_txs = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        let (tx, rx) = mpsc::channel::<BrokerMessage>();
        thread::spawn(move || run_broker(rx));
        shard_txs.push(tx);
    }
    ShardedBroker { shard_txs: Arc::new(shard_txs) }
}

// ============================================================================

// BlitzBroker entry point: parses CLI args, starts the broker actor
// thread, and runs the TCP accept loop, spawning a connection handler
// per client. See PLAN.md for architecture and DECISIONS.md for the
// concurrency-model reasoning.




struct Config {
    host: String,
    port: u16,
}

/// Hand-rolled CLI parsing — see STDLIB.md (`clap` substitution). Accepts
/// `--host <addr>` and `--port <port>`; both optional with sane
/// defaults.
fn parse_args() -> Config {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 1883; // MQTT's conventional default port

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => {
                if let Some(v) = args.get(i + 1) {
                    host = v.clone();
                    i += 1;
                }
            }
            "--port" => {
                if let Some(v) = args.get(i + 1) {
                    if let Ok(p) = v.parse() {
                        port = p;
                    }
                    i += 1;
                }
            }
            other => {
                warn(&format!("ignoring unrecognized argument: {other}"));
            }
        }
        i += 1;
    }

    Config { host, port }
}

fn main() {
    let config = parse_args();
    let addr = format!("{}:{}", config.host, config.port);

    // Spawn N independent broker threads (one per shard). Each owns a
    // disjoint subset of topics — see DECISIONS.md #1 and PLAN.md §4 item 3.
    let broker = spawn_sharded_broker(NUM_BROKER_SHARDS);
    info(&format!("BlitzBroker: {NUM_BROKER_SHARDS} broker shards active"));

    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            error(&format!("failed to bind {addr}: {e}"));
            std::process::exit(1);
        }
    };
    info(&format!("BlitzBroker listening on {addr}"));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // ShardedBroker is cheaply cloneable (Arc<Vec<Sender>>).
                let broker = broker.clone();
                thread::spawn(move || {
                    handle_connection(stream, broker);
                });
            }
            Err(e) => {
                warn(&format!("accept error: {e}"));
            }
        }
    }

    // Only reached if the listener stops iterating (shouldn't happen in
    // normal operation). Dropping the ShardedBroker closes all shard
    // channels, allowing each broker thread to exit cleanly.
    drop(broker);
}

// ============================================================================

// Role C: packet-parsing edge-case tests belong here (truncated
// input, invalid remaining-length encoding, oversized payloads,
// malformed UTF-8 in string fields, etc). Write these against the
// spec before Role B's implementation lands — see PLAN.md §8's
// hour-0/parallel guidance. `#[should_panic]` is NOT the right tool
// for testing rejection paths — decode() must return Err, not panic;
// a test that panics because decode() panicked is not exercising
// AI_GUARDRAILS.md rule 3.
//
// Role B's own tests below cover the functions this file owns
// directly (remaining-length codec, round-trip encode/decode, and
// the rejection paths implemented so far) — these don't replace
// Role C's broader edge-case/fuzzing pass, just prove Role B's own
// implementation against the spec sections it cites.

// --- Remaining Length, §2.2.3 ---

#[cfg(test)]
#[test]
fn remaining_length_roundtrip_single_byte() {
    // Spec example: 64 encodes as a single byte 0x40.
    let encoded = encode_remaining_length(64);
    assert_eq!(encoded, vec![0x40]);
    assert_eq!(decode_remaining_length(&encoded), Ok((64, 1)));
}

#[cfg(test)]
#[test]
fn remaining_length_roundtrip_two_bytes() {
    // Spec example: 321 encodes as 0xC1 0x02.
    let encoded = encode_remaining_length(321);
    assert_eq!(encoded, vec![0xC1, 0x02]);
    assert_eq!(decode_remaining_length(&encoded), Ok((321, 2)));
}

#[cfg(test)]
#[test]
fn remaining_length_max_value_four_bytes() {
    // Spec max: 268,435,455 (0xFF 0xFF 0xFF 0x7F).
    let encoded = encode_remaining_length(268_435_455);
    assert_eq!(encoded, vec![0xFF, 0xFF, 0xFF, 0x7F]);
    assert_eq!(decode_remaining_length(&encoded), Ok((268_435_455, 4)));
}

#[cfg(test)]
#[test]
fn remaining_length_incomplete_never_panics() {
    // A continuation byte with nothing after it: not malformed yet,
    // just not fully arrived.
    assert!(matches!(
        decode_remaining_length(&[0x80]),
        Err(ProtocolError::Incomplete)
    ));
    assert!(matches!(decode_remaining_length(&[]), Err(ProtocolError::Incomplete)));
}

#[cfg(test)]
#[test]
fn remaining_length_rejects_five_continuation_bytes() {
    // §2.2.3: malformed if it would need a 5th byte.
    let malformed = [0xFF, 0xFF, 0xFF, 0xFF, 0x01];
    assert!(matches!(
        decode_remaining_length(&malformed),
        Err(ProtocolError::InvalidRemainingLength)
    ));
}

// --- decode() top-level framing ---

#[cfg(test)]
#[test]
fn decode_empty_buffer_is_incomplete_not_panic() {
    assert!(matches!(decode(&[]), Err(ProtocolError::Incomplete)));
}

#[cfg(test)]
#[test]
fn decode_unknown_packet_type_is_rejected() {
    // Packet type nibble 0 is reserved / unused in MQTT 3.1.1.
    let buf = [0x00u8, 0x00];
    assert!(matches!(
        decode(&buf),
        Err(ProtocolError::UnknownPacketType(0))
    ));
}

#[cfg(test)]
#[test]
fn decode_truncated_after_fixed_header_is_incomplete() {
    // Claims 10 remaining bytes but buffer only has the header.
    let buf = [(PT_PINGREQ << 4), 10];
    assert!(matches!(decode(&buf), Err(ProtocolError::Incomplete)));
}

#[cfg(test)]
#[test]
fn decode_pingreq_roundtrip() {
    let bytes = encode(&MqttPacket::PingReq);
    let (packet, consumed) = decode(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    assert!(matches!(packet, MqttPacket::PingReq));
}

#[cfg(test)]
#[test]
fn decode_pingreq_rejects_nonzero_remaining_length() {
    // §3.13.1: PINGREQ has no variable header or payload.
    let buf = [(PT_PINGREQ << 4), 0x01, 0xAB];
    assert!(matches!(decode(&buf), Err(ProtocolError::MalformedPayload(_))));
}

// --- UTF-8 string field, §1.5.3 ---

#[cfg(test)]
#[test]
fn utf8_string_rejects_truncated_length_prefix() {
    assert!(decode_utf8_string(&[0x00]).is_err());
}

#[cfg(test)]
#[test]
fn utf8_string_rejects_length_exceeding_buffer() {
    // Claims 100 bytes follow but only 2 are present.
    let buf = [0x00, 0x64, b'h', b'i'];
    assert!(decode_utf8_string(&buf).is_err());
}

#[cfg(test)]
#[test]
fn utf8_string_rejects_invalid_utf8_bytes() {
    let buf = [0x00, 0x02, 0xFF, 0xFE];
    assert!(decode_utf8_string(&buf).is_err());
}

// --- CONNECT, §3.1 ---

#[cfg(test)]
fn sample_connect() -> ConnectPacket {
    ConnectPacket {
        protocol_name: "MQTT".to_string(),
        protocol_level: 4,
        clean_session: true,
        keep_alive_secs: 60,
        client_id: "test-client".to_string(),
    }
}

#[cfg(test)]
#[test]
fn connect_roundtrip() {
    let original = MqttPacket::Connect(sample_connect());
    let bytes = encode(&original);
    let (decoded, consumed) = decode(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        MqttPacket::Connect(p) => {
            assert_eq!(p.protocol_name, "MQTT");
            assert_eq!(p.protocol_level, 4);
            assert!(p.clean_session);
            assert_eq!(p.keep_alive_secs, 60);
            assert_eq!(p.client_id, "test-client");
        }
        _ => panic!("expected Connect"),
    }
}

#[cfg(test)]
#[test]
fn connect_rejects_wrong_protocol_name() {
    let mut body = Vec::new();
    encode_utf8_string("MQXX", &mut body);
    body.push(4); // protocol level
    body.push(0x02); // clean session
    body.extend_from_slice(&60u16.to_be_bytes());
    encode_utf8_string("id", &mut body);
    let bytes = prepend_fixed_header(PT_CONNECT, 0, body);
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn connect_rejects_unsupported_protocol_level() {
    let mut body = Vec::new();
    encode_utf8_string("MQTT", &mut body);
    body.push(5); // not level 4
    body.push(0x02);
    body.extend_from_slice(&60u16.to_be_bytes());
    encode_utf8_string("id", &mut body);
    let bytes = prepend_fixed_header(PT_CONNECT, 0, body);
    assert!(matches!(decode(&bytes), Err(ProtocolError::UnsupportedFeature(_))));
}

#[cfg(test)]
#[test]
fn connect_truncated_mid_variable_header_is_malformed_not_panic() {
    // Only the protocol name, nothing else — decode() must return
    // an Err, never panic/index-out-of-bounds.
    let mut body = Vec::new();
    encode_utf8_string("MQTT", &mut body);
    let bytes = prepend_fixed_header(PT_CONNECT, 0, body);
    assert!(decode(&bytes).is_err());
}

// --- SUBSCRIBE / UNSUBSCRIBE, §3.8 / §3.10 ---

#[cfg(test)]
#[test]
fn subscribe_roundtrip_multiple_topics() {
    let original = MqttPacket::Subscribe(SubscribePacket {
        packet_id: 42,
        subscriptions: vec![("a/b".to_string(), 0), ("c/d".to_string(), 1)],
    });
    let bytes = encode(&original);
    let (decoded, consumed) = decode(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        MqttPacket::Subscribe(p) => {
            assert_eq!(p.packet_id, 42);
            assert_eq!(p.subscriptions, vec![("a/b".to_string(), 0), ("c/d".to_string(), 1)]);
        }
        _ => panic!("expected Subscribe"),
    }
}

#[cfg(test)]
#[test]
fn subscribe_rejects_empty_topic_list() {
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes());
    let bytes = prepend_fixed_header(PT_SUBSCRIBE, 0b0010, body);
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn subscribe_rejects_invalid_qos() {
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes());
    encode_utf8_string("a", &mut body);
    body.push(3); // invalid QoS
    let bytes = prepend_fixed_header(PT_SUBSCRIBE, 0b0010, body);
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn unsubscribe_roundtrip() {
    let original = MqttPacket::Unsubscribe(UnsubscribePacket {
        packet_id: 7,
        topic_filters: vec!["x/y".to_string()],
    });
    let bytes = encode(&original);
    let (decoded, _) = decode(&bytes).unwrap();
    match decoded {
        MqttPacket::Unsubscribe(p) => {
            assert_eq!(p.packet_id, 7);
            assert_eq!(p.topic_filters, vec!["x/y".to_string()]);
        }
        _ => panic!("expected Unsubscribe"),
    }
}

// --- PUBLISH, §3.3 (QoS 0 core scope) ---

#[cfg(test)]
#[test]
fn publish_qos0_roundtrip() {
    let original = MqttPacket::Publish(PublishPacket {
        topic: "weather".to_string(),
        payload: b"rain".to_vec(),
        qos: 0,
        retain: false,
        packet_id: None,
    });
    let bytes = encode(&original);
    let (decoded, consumed) = decode(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        MqttPacket::Publish(p) => {
            assert_eq!(p.topic, "weather");
            assert_eq!(p.payload, b"rain");
            assert_eq!(p.qos, 0);
            assert!(!p.retain);
            assert_eq!(p.packet_id, None);
        }
        _ => panic!("expected Publish"),
    }
}

#[cfg(test)]
#[test]
fn publish_empty_payload_is_valid() {
    // §3.3.3: a zero-length payload is legal.
    let original = MqttPacket::Publish(PublishPacket {
        topic: "t".to_string(),
        payload: vec![],
        qos: 0,
        retain: false,
        packet_id: None,
    });
    let bytes = encode(&original);
    let (decoded, _) = decode(&bytes).unwrap();
    match decoded {
        MqttPacket::Publish(p) => assert!(p.payload.is_empty()),
        _ => panic!("expected Publish"),
    }
}

#[cfg(test)]
#[test]
fn publish_rejects_qos2_reserved_value() {
    let mut body = Vec::new();
    encode_utf8_string("t", &mut body);
    // flags: QoS bits = 11 (3), the reserved/invalid combination.
    let bytes = prepend_fixed_header(PT_PUBLISH, 0b0110, body);
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn publish_rejects_qos2_as_unsupported() {
    let mut body = Vec::new();
    encode_utf8_string("t", &mut body);
    body.extend_from_slice(&1u16.to_be_bytes()); // packet id, if it were QoS2
    let bytes = prepend_fixed_header(PT_PUBLISH, 0b0100, body); // QoS bits = 10
    assert!(matches!(decode(&bytes), Err(ProtocolError::UnsupportedFeature(_))));
}

#[cfg(test)]
#[test]
fn publish_qos1_roundtrip() {
    let original = MqttPacket::Publish(PublishPacket {
        topic: "weather".to_string(),
        payload: b"rain".to_vec(),
        qos: 1,
        retain: false,
        packet_id: Some(42),
    });
    let bytes = encode(&original);
    let (decoded, consumed) = decode(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        MqttPacket::Publish(p) => {
            assert_eq!(p.qos, 1);
            assert_eq!(p.packet_id, Some(42));
            assert_eq!(p.payload, b"rain");
        }
        _ => panic!("expected Publish"),
    }
}

#[cfg(test)]
#[test]
fn publish_qos1_rejects_truncated_packet_identifier() {
    let mut body = Vec::new();
    encode_utf8_string("t", &mut body);
    // Only 1 byte of what should be a 2-byte packet identifier.
    body.push(0x00);
    let bytes = prepend_fixed_header(PT_PUBLISH, 0b0010, body); // QoS bits = 01
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn publish_qos1_rejects_zero_packet_identifier() {
    let mut body = Vec::new();
    encode_utf8_string("t", &mut body);
    body.extend_from_slice(&0u16.to_be_bytes()); // packet id 0 — illegal
    let bytes = prepend_fixed_header(PT_PUBLISH, 0b0010, body);
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn puback_roundtrip() {
    let original = MqttPacket::PubAck(PubAckPacket { packet_id: 99 });
    let bytes = encode(&original);
    let (decoded, consumed) = decode(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        MqttPacket::PubAck(p) => assert_eq!(p.packet_id, 99),
        _ => panic!("expected PubAck"),
    }
}

#[cfg(test)]
#[test]
fn puback_rejects_wrong_body_length() {
    // 3 bytes instead of the required exactly-2.
    let bytes = prepend_fixed_header(PT_PUBACK, 0, vec![0x00, 0x01, 0x02]);
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn puback_rejects_zero_packet_identifier() {
    let bytes = prepend_fixed_header(PT_PUBACK, 0, 0u16.to_be_bytes().to_vec());
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn puback_rejects_nonzero_flags() {
    // Fixed header flags for PUBACK are reserved and must be 0
    // (§3.4.1) — construct one with a nonzero flag nibble by hand.
    let mut bytes = prepend_fixed_header(PT_PUBACK, 0, 2u16.to_be_bytes().to_vec());
    bytes[0] |= 0b0001; // set a reserved flag bit
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn publish_rejects_empty_topic_name() {
    let mut body = Vec::new();
    encode_utf8_string("", &mut body);
    let bytes = prepend_fixed_header(PT_PUBLISH, 0, body);
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn publish_rejects_wildcard_in_topic_name() {
    let mut body = Vec::new();
    encode_utf8_string("a/+/b", &mut body);
    let bytes = prepend_fixed_header(PT_PUBLISH, 0, body);
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

// --- Topic wildcards, §4.7 ---

#[cfg(test)]
#[test]
fn validate_filter_accepts_plain_topic() {
    assert!(validate_topic_filter("sport/tennis/player1").is_ok());
}

#[cfg(test)]
#[test]
fn validate_filter_accepts_hash_alone() {
    assert!(validate_topic_filter("#").is_ok());
}

#[cfg(test)]
#[test]
fn validate_filter_accepts_hash_as_last_level() {
    assert!(validate_topic_filter("sport/tennis/player1/#").is_ok());
}

#[cfg(test)]
#[test]
fn validate_filter_rejects_hash_not_last() {
    assert!(matches!(
        validate_topic_filter("sport/#/player1"),
        Err(ProtocolError::MalformedPayload(_))
    ));
}

#[cfg(test)]
#[test]
fn validate_filter_rejects_hash_not_alone_in_level() {
    assert!(matches!(
        validate_topic_filter("sport/tennis#"),
        Err(ProtocolError::MalformedPayload(_))
    ));
}

#[cfg(test)]
#[test]
fn validate_filter_accepts_plus_at_any_level() {
    assert!(validate_topic_filter("sport/+/player1").is_ok());
    assert!(validate_topic_filter("+/+").is_ok());
    assert!(validate_topic_filter("+").is_ok());
}

#[cfg(test)]
#[test]
fn validate_filter_rejects_plus_not_alone_in_level() {
    assert!(matches!(
        validate_topic_filter("sport/+aa/player1"),
        Err(ProtocolError::MalformedPayload(_))
    ));
}

#[cfg(test)]
#[test]
fn validate_filter_rejects_empty_filter() {
    assert!(matches!(
        validate_topic_filter(""),
        Err(ProtocolError::MalformedPayload(_))
    ));
}

#[cfg(test)]
#[test]
fn matches_exact_topic() {
    assert!(topic_matches_filter("sport/tennis/player1", "sport/tennis/player1"));
    assert!(!topic_matches_filter("sport/tennis/player1", "sport/tennis/player2"));
}

#[cfg(test)]
#[test]
fn matches_multi_level_wildcard() {
    assert!(topic_matches_filter("sport/tennis/player1", "sport/tennis/player1/#"));
    assert!(topic_matches_filter(
        "sport/tennis/player1/ranking",
        "sport/tennis/player1/#"
    ));
    assert!(topic_matches_filter("sport", "sport/#"));
    assert!(topic_matches_filter("sport/anything/at/all", "sport/#"));
    assert!(topic_matches_filter("anything", "#"));
}

#[cfg(test)]
#[test]
fn matches_single_level_wildcard() {
    assert!(topic_matches_filter("sport/tennis/player1", "sport/+/player1"));
    assert!(topic_matches_filter("sport/hockey/player1", "sport/+/player1"));
    // '+' matches exactly one level — must not also swallow a
    // deeper level.
    assert!(!topic_matches_filter(
        "sport/tennis/junior/player1",
        "sport/+/player1"
    ));
}

#[cfg(test)]
#[test]
fn matches_plus_does_not_match_missing_level() {
    // "sport/+" requires a second level to exist; "sport" alone
    // must not match.
    assert!(!topic_matches_filter("sport", "sport/+"));
}

#[cfg(test)]
#[test]
fn matches_leading_slash_topics() {
    // "+/+" matches "/finance" (an empty first level, per the
    // spec's own worked example in §4.7.1.3).
    assert!(topic_matches_filter("/finance", "+/+"));
}

#[cfg(test)]
#[test]
fn matches_never_panics_on_adversarial_slash_counts() {
    // Regression-style guard: matching must stay well-behaved even
    // on a topic/filter with a large number of levels, since both
    // strings are attacker-influenced (arrive over the wire).
    let many_slashes = "a/".repeat(10_000);
    assert!(!topic_matches_filter(&many_slashes, "x/y/z"));
    assert!(topic_matches_filter(&many_slashes, "#"));
}

#[cfg(test)]
#[test]
fn subscribe_rejects_invalid_wildcard_filter() {
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes());
    encode_utf8_string("sport/tennis#", &mut body); // invalid: # not alone
    body.push(0);
    let bytes = prepend_fixed_header(PT_SUBSCRIBE, 0b0010, body);
    assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
}

#[cfg(test)]
#[test]
fn subscribe_accepts_valid_wildcard_filter() {
    let original = MqttPacket::Subscribe(SubscribePacket {
        packet_id: 1,
        subscriptions: vec![("sport/+/player1".to_string(), 0)],
    });
    let bytes = encode(&original);
    assert!(decode(&bytes).is_ok());
}

// --- Buffering behavior connection.rs relies on ---

#[cfg(test)]
#[test]
fn decode_never_panics_on_random_bytes() {
    // Regression test: an earlier version of decode_remaining_length
    // overflowed on a malformed 5-continuation-byte sequence,
    // violating AI_GUARDRAILS.md rule 3 (no panics on untrusted
    // input). Cheap deterministic PRNG (xorshift) so this needs no
    // crate.
    let mut state: u32 = 0x1234_5678;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        state
    };

    for _ in 0..20_000 {
        let len = (next() % 64) as usize;
        let buf: Vec<u8> = (0..len).map(|_| (next() % 256) as u8).collect();
        let _ = decode(&buf); // must return Ok or Err, never panic
    }
}

#[cfg(test)]
#[test]
fn decode_only_consumes_one_packet_leaves_rest_buffered() {
    let mut buf = encode(&MqttPacket::PingReq);
    let second = encode(&MqttPacket::PingReq);
    buf.extend_from_slice(&second);

    let (_, consumed) = decode(&buf).unwrap();
    assert_eq!(consumed, buf.len() - second.len());
    // Caller (connection.rs) drains `consumed` bytes and calls
    // decode() again on the remainder — verify that remainder is
    // itself still a valid, decodable packet.
    let (_, consumed2) = decode(&buf[consumed..]).unwrap();
    assert_eq!(consumed2, second.len());
}

// --- Role C: packet-parsing edge cases (overflow guard, large payload) ---

#[cfg(test)]
#[test]
fn decode_max_remaining_length_never_panics() {
    // Exercises the `header_len.checked_add(remaining_len as usize)`
    // guard in decode() (§2.2.3 / AI_GUARDRAILS.md rule 3).
    //
    // The MQTT 3.1.1 spec max remaining length is 268,435,455, encoded
    // as four bytes [0xFF, 0xFF, 0xFF, 0x7F] (§2.2.3 Table 2.4).  We
    // supply only a 5-byte buffer (1 fixed-header byte + 4 RL bytes,
    // no payload), so decode() must return an Err without panicking.
    //
    // Platform note: on 64-bit hosts `remaining_len as usize`
    // (268,435,455) + `header_len` (5) = 268,435,460, which is well
    // within usize range, so the overflow branch itself is not
    // reachable on this target.  The checked_add succeeds and the
    // function returns Err(ProtocolError::Incomplete) because
    // buf.len() < total_len.  The intent of this test is to confirm
    // that the code path runs to completion and returns an Err —
    // never panics — regardless of which Err variant is produced.
    // On a hypothetical 16-bit target the overflow branch would be
    // reached instead, returning MalformedPayload; the is_err()
    // assertion covers both outcomes.  See Personal_Decisions.md for
    // the full judgment-call record.
    let buf: [u8; 5] = [
        PT_PUBLISH << 4,  // packet type: PUBLISH, flags 0 (QoS 0)
        0xFF,             // remaining length byte 1: continuation set
        0xFF,             // remaining length byte 2: continuation set
        0xFF,             // remaining length byte 3: continuation set
        0x7F,             // remaining length byte 4: no continuation, value = 268,435,455
    ];
    // Must return an Err — never panic, never index out of bounds.
    assert!(decode(&buf).is_err());
}

#[cfg(test)]
#[test]
fn decode_large_publish_payload_roundtrip() {
    // Confirms that a well-formed but large PUBLISH payload (1 MiB)
    // decodes correctly and that the round-trip is byte-exact
    // (AI_GUARDRAILS.md rule 3: no panic on oversized-but-valid input).
    //
    // Topic: "load/test" (9 bytes); payload: 1,048,576 bytes, all 0xAB.
    // Resulting remaining length = 2 + 9 + 1,048,576 = 1,048,587,
    // which encodes as 4 MQTT RL bytes (§2.2.3), well within the
    // spec max of 268,435,455.
    let topic = "load/test";
    let payload: Vec<u8> = vec![0xAB; 1024 * 1024]; // 1 MiB
    let original = MqttPacket::Publish(PublishPacket {
        topic: topic.to_string(),
        payload: payload.clone(),
        qos: 0,
        retain: false,
        packet_id: None,
    });
    let bytes = encode(&original);
    let (decoded, consumed) = decode(&bytes).expect("large PUBLISH must decode without error");
    assert_eq!(consumed, bytes.len(), "consumed must equal the full encoded length");
    match decoded {
        MqttPacket::Publish(p) => {
            assert_eq!(p.topic, topic);
            assert_eq!(p.payload.len(), payload.len(), "payload length must be preserved");
            assert_eq!(p.payload, payload, "payload bytes must be preserved exactly");
            assert_eq!(p.qos, 0);
            assert!(!p.retain);
            assert_eq!(p.packet_id, None);
        }
        _ => panic!("expected Publish"),
    }
}

// ============================================================================

#[cfg(test)]
#[test]
fn drop_oldest_when_full() {
    let q: QueueHandle<i32> = new(2);
    assert!(!q.push(1));
    assert!(!q.push(2));
    assert!(q.push(3)); // capacity 2, pushing a 3rd drops the oldest (1)
    assert_eq!(q.pop_blocking(), Some(2));
    assert_eq!(q.pop_blocking(), Some(3));
}

#[cfg(test)]
#[test]
fn close_wakes_blocked_consumer() {
    let q: QueueHandle<i32> = new(4);
    q.close();
    assert_eq!(q.pop_blocking(), None);
}

#[cfg(test)]
#[test]
fn fifo_order_preserved_under_capacity() {
    let q: QueueHandle<i32> = new(10);
    for i in 0..5 {
        q.push(i);
    }
    for i in 0..5 {
        assert_eq!(q.pop_blocking(), Some(i));
    }
}

// ============================================================================

/// Pop the next outbound packet from the queue. The queue must be
/// closed first so pop_blocking doesn't block indefinitely when empty.
#[cfg(test)]
fn pop_packet(q: &QueueHandle<OutboundEvent>) -> MqttPacket {
    match q.pop_blocking() {
        Some(OutboundEvent::Packet(p)) => p,
        None => panic!("outbound queue was empty — expected a packet"),
    }
}

// -----------------------------------------------------------------------
// SUBSCRIBE → SUBACK   (MQTT 3.1.1 §3.8 / §3.9)
// -----------------------------------------------------------------------

/// A SUBSCRIBE with a single topic must produce a SUBACK whose
/// packet_id mirrors the SUBSCRIBE's packet_id and whose return_codes
/// slice contains exactly one 0x00 (QoS 0 granted, §3.9.3 Table 3.4).
#[cfg(test)]
#[test]
fn subscribe_single_topic_produces_suback_with_correct_packet_id_and_return_code() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let outbound = new(DEFAULT_CLIENT_QUEUE_CAPACITY);

    let sub = SubscribePacket {
        packet_id: 5,
        subscriptions: vec![("sensors/temp".to_string(), 0)],
    };
    let result = dispatch_packet(1, MqttPacket::Subscribe(sub), &tx, &outbound);

    assert!(result, "dispatch_packet must return true for SUBSCRIBE");

    // Broker must have received exactly one Subscribe message.
    let msg = rx.try_recv().expect("broker should have received BrokerMessage::Subscribe");
    match msg {
        BrokerMessage::Subscribe { id: _, topic } => {
            assert_eq!(topic, "sensors/temp");
        }
        _ => panic!("expected BrokerMessage::Subscribe"),
    }
    // No extra broker messages.
    assert!(rx.try_recv().is_err(), "only one BrokerMessage::Subscribe expected");

    outbound.close();
    match pop_packet(&outbound) {
        MqttPacket::SubAck(ack) => {
            assert_eq!(
                ack.packet_id, 5,
                "SUBACK packet_id must match SUBSCRIBE packet_id (§3.9.2)"
            );
            assert_eq!(
                ack.return_codes,
                vec![0x00],
                "one 0x00 return code per topic (§3.9.3 Table 3.4: QoS 0 granted)"
            );
        }
        other => panic!("expected SubAck, got {:?}", other),
    }
}

/// A SUBSCRIBE with multiple topics must produce a SUBACK with one
/// return code per topic, in the same order as the subscriptions
/// (MQTT 3.1.1 §3.9.3: "The return codes must be listed in the same
/// order as the list of Topic Filters in the SUBSCRIBE packet").
#[cfg(test)]
#[test]
fn subscribe_multiple_topics_produces_suback_with_one_code_per_topic_in_order() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let outbound = new(DEFAULT_CLIENT_QUEUE_CAPACITY);

    let sub = SubscribePacket {
        packet_id: 99,
        subscriptions: vec![
            ("a/b".to_string(), 0u8),
            ("c/d".to_string(), 0u8),
            ("e/f".to_string(), 0u8),
        ],
    };
    dispatch_packet(1, MqttPacket::Subscribe(sub), &tx, &outbound);

    // Verify broker received exactly three Subscribe messages, in order.
    for expected in &["a/b", "c/d", "e/f"] {
        let msg = rx.try_recv().expect("expected a BrokerMessage::Subscribe");
        match msg {
            BrokerMessage::Subscribe { id: _, topic } => {
                assert_eq!(&topic, expected);
            }
            _ => panic!("expected BrokerMessage::Subscribe"),
        }
    }
    assert!(rx.try_recv().is_err(), "no extra broker messages expected");

    outbound.close();
    match pop_packet(&outbound) {
        MqttPacket::SubAck(ack) => {
            assert_eq!(ack.packet_id, 99);
            assert_eq!(
                ack.return_codes,
                vec![0x00, 0x00, 0x00],
                "one return code per subscription, same order (§3.9.3)"
            );
        }
        other => panic!("expected SubAck, got {:?}", other),
    }
}

/// Packet ID wrapping: ensure a SUBSCRIBE with the max packet_id
/// (0xFFFF) is echoed correctly — no truncation or overflow.
#[cfg(test)]
#[test]
fn subscribe_max_packet_id_echoed_in_suback() {
    let (tx, _rx) = mpsc::channel::<BrokerMessage>();
    let outbound = new(DEFAULT_CLIENT_QUEUE_CAPACITY);

    let sub = SubscribePacket {
        packet_id: 0xFFFF,
        subscriptions: vec![("t".to_string(), 0)],
    };
    dispatch_packet(1, MqttPacket::Subscribe(sub), &tx, &outbound);

    outbound.close();
    match pop_packet(&outbound) {
        MqttPacket::SubAck(ack) => {
            assert_eq!(ack.packet_id, 0xFFFF, "packet_id 0xFFFF must be echoed verbatim");
        }
        other => panic!("expected SubAck, got {:?}", other),
    }
}

// -----------------------------------------------------------------------
// UNSUBSCRIBE → UNSUBACK   (MQTT 3.1.1 §3.10 / §3.11)
// -----------------------------------------------------------------------

/// A UNSUBSCRIBE must produce an UNSUBACK whose packet_id mirrors
/// the UNSUBSCRIBE's packet_id (§3.11: fixed-format, no payload).
#[cfg(test)]
#[test]
fn unsubscribe_produces_unsuback_with_correct_packet_id() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let outbound = new(DEFAULT_CLIENT_QUEUE_CAPACITY);

    let unsub = UnsubscribePacket {
        packet_id: 12,
        topic_filters: vec!["sensors/temp".to_string()],
    };
    let result = dispatch_packet(1, MqttPacket::Unsubscribe(unsub), &tx, &outbound);

    assert!(result, "dispatch_packet must return true for UNSUBSCRIBE");

    let msg = rx.try_recv().expect("broker should have received BrokerMessage::Unsubscribe");
    match msg {
        BrokerMessage::Unsubscribe { id: _, topic } => {
            assert_eq!(topic, "sensors/temp");
        }
        _ => panic!("expected BrokerMessage::Unsubscribe"),
    }
    assert!(rx.try_recv().is_err(), "no extra broker messages expected");

    outbound.close();
    match pop_packet(&outbound) {
        MqttPacket::UnsubAck(ack) => {
            assert_eq!(
                ack.packet_id, 12,
                "UNSUBACK packet_id must match UNSUBSCRIBE packet_id (§3.11.2)"
            );
        }
        other => panic!("expected UnsubAck, got {:?}", other),
    }
}

/// Multiple topic filters in one UNSUBSCRIBE: all must be forwarded
/// to the broker, and exactly one UNSUBACK is sent (§3.11).
#[cfg(test)]
#[test]
fn unsubscribe_multiple_topics_sends_one_unsuback() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let outbound = new(DEFAULT_CLIENT_QUEUE_CAPACITY);

    let unsub = UnsubscribePacket {
        packet_id: 7,
        topic_filters: vec!["a/b".to_string(), "c/d".to_string()],
    };
    dispatch_packet(1, MqttPacket::Unsubscribe(unsub), &tx, &outbound);

    // Two broker Unsubscribe messages, one per topic.
    for expected in &["a/b", "c/d"] {
        let msg = rx.try_recv().expect("expected BrokerMessage::Unsubscribe");
        match msg {
            BrokerMessage::Unsubscribe { id: _, topic } => {
                assert_eq!(&topic, expected);
            }
            _ => panic!("expected BrokerMessage::Unsubscribe"),
        }
    }
    assert!(rx.try_recv().is_err(), "no extra broker messages expected");

    outbound.close();
    // Exactly one UNSUBACK.
    match pop_packet(&outbound) {
        MqttPacket::UnsubAck(ack) => {
            assert_eq!(ack.packet_id, 7);
        }
        other => panic!("expected UnsubAck, got {:?}", other),
    }
    // No second packet queued.
    assert!(
        outbound.pop_blocking().is_none(),
        "only one UNSUBACK must be queued for a single UNSUBSCRIBE"
    );
}

// -----------------------------------------------------------------------
// PUBLISH (QoS 1) → PUBACK   (MQTT 3.1.1 §3.3.4)
//
// Regression tests for the bug Role A caught via a live
// `mosquitto_pub -q 1` run hanging against the real binary: the
// broker never sent a PUBACK back to a QoS 1 publisher. The earlier
// protocol.rs encode/decode tests for QoS 1 couldn't have caught
// this — they only prove PUBACK's *wire format* is correct, not
// that the broker's dispatch logic actually sends one. These tests
// exercise dispatch_packet directly (no real socket needed, same
// pattern as the SUBSCRIBE/UNSUBSCRIBE tests above) so this
// specific class of bug can't silently regress again.
// -----------------------------------------------------------------------

/// A QoS 1 PUBLISH must produce a PUBACK on the *publishing*
/// client's own outbound queue, echoing that PUBLISH's packet_id
/// (§3.3.4) — this is the exact scenario that was broken.
#[cfg(test)]
#[test]
fn publish_qos1_produces_puback_with_matching_packet_id() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let outbound = new(DEFAULT_CLIENT_QUEUE_CAPACITY);

    let publish = PublishPacket {
        topic: "sensors/temp".to_string(),
        payload: b"21C".to_vec(),
        qos: 1,
        retain: false,
        packet_id: Some(42),
    };
    let result = dispatch_packet(1, MqttPacket::Publish(publish), &tx, &outbound);

    assert!(result, "dispatch_packet must return true for PUBLISH");

    // Broker must still receive the Publish message for fan-out —
    // the ack fix must not have replaced that forwarding.
    let msg = rx.try_recv().expect("broker should have received BrokerMessage::Publish");
    match msg {
        BrokerMessage::Publish { from: _, packet } => {
            assert_eq!(packet.topic, "sensors/temp");
            assert_eq!(packet.qos, 1);
        }
        _ => panic!("expected BrokerMessage::Publish"),
    }
    assert!(rx.try_recv().is_err(), "only one BrokerMessage::Publish expected");

    outbound.close();
    match pop_packet(&outbound) {
        MqttPacket::PubAck(ack) => {
            assert_eq!(
                ack.packet_id, 42,
                "PUBACK packet_id must match the PUBLISH's packet_id (§3.3.4)"
            );
        }
        other => panic!("expected PubAck on the publishing client's own queue, got {:?}", other),
    }
    // Exactly one packet queued — no duplicate acks.
    assert!(
        outbound.pop_blocking().is_none(),
        "only one PUBACK must be queued for a single QoS 1 PUBLISH"
    );
}

/// A QoS 0 PUBLISH must NOT produce any PUBACK (§3.3.4 — acks only
/// apply to QoS > 0). Guards against a naive fix that acks
/// unconditionally regardless of QoS.
#[cfg(test)]
#[test]
fn publish_qos0_produces_no_puback() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let outbound = new(DEFAULT_CLIENT_QUEUE_CAPACITY);

    let publish = PublishPacket {
        topic: "sensors/temp".to_string(),
        payload: b"21C".to_vec(),
        qos: 0,
        retain: false,
        packet_id: None,
    };
    let result = dispatch_packet(1, MqttPacket::Publish(publish), &tx, &outbound);

    assert!(result, "dispatch_packet must return true for PUBLISH");
    assert!(rx.try_recv().is_ok(), "broker should still receive BrokerMessage::Publish for QoS 0");

    outbound.close();
    assert!(
        outbound.pop_blocking().is_none(),
        "QoS 0 PUBLISH must not produce any outbound packet (no PUBACK for QoS 0, §3.3.4)"
    );
}

/// Two QoS 1 PUBLISHes with different packet identifiers on the
/// same connection must each get their own correctly-matched
/// PUBACK, in order — guards against a fix that hardcodes or
/// reuses a single packet_id.
#[cfg(test)]
#[test]
fn multiple_qos1_publishes_each_get_correctly_matched_puback() {
    let (tx, _rx) = mpsc::channel::<BrokerMessage>();
    let outbound = new(DEFAULT_CLIENT_QUEUE_CAPACITY);

    for pid in [1u16, 2, 3] {
        let publish = PublishPacket {
            topic: "t".to_string(),
            payload: vec![],
            qos: 1,
            retain: false,
            packet_id: Some(pid),
        };
        dispatch_packet(1, MqttPacket::Publish(publish), &tx, &outbound);
    }

    outbound.close();
    for expected_pid in [1u16, 2, 3] {
        match pop_packet(&outbound) {
            MqttPacket::PubAck(ack) => assert_eq!(ack.packet_id, expected_pid),
            other => panic!("expected PubAck({expected_pid}), got {:?}", other),
        }
    }
    assert!(outbound.pop_blocking().is_none(), "no extra packets expected");
}

// ============================================================================

#[cfg(test)]
#[test]
fn publish_fans_out_to_all_subscribers() {
    let (tx, rx) = mpsc::channel();
    let broker = thread::spawn(move || run_broker(rx));

    let out_a = new::<OutboundEvent>(4);
    let out_b = new::<OutboundEvent>(4);

    tx.send(BrokerMessage::Register {
        id: 1,
        client_id: "a".into(),
        outbound: out_a.clone(),
    })
    .unwrap();
    tx.send(BrokerMessage::Register {
        id: 2,
        client_id: "b".into(),
        outbound: out_b.clone(),
    })
    .unwrap();
    tx.send(BrokerMessage::Subscribe {
        id: 1,
        topic: "weather".into(),
    })
    .unwrap();
    tx.send(BrokerMessage::Subscribe {
        id: 2,
        topic: "weather".into(),
    })
    .unwrap();
    tx.send(BrokerMessage::Publish {
        from: 1,
        packet: PublishPacket {
            topic: "weather".into(),
            payload: b"rain".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    })
    .unwrap();

    let got_a = out_a.pop_blocking().expect("subscriber a should receive the publish");
    let got_b = out_b.pop_blocking().expect("subscriber b should receive the publish");
    for got in [got_a, got_b] {
        match got {
            OutboundEvent::Packet(MqttPacket::Publish(p)) => {
                assert_eq!(p.topic, "weather");
                assert_eq!(p.payload, b"rain");
            }
            _ => panic!("expected a Publish packet"),
        }
    }

    drop(tx);
    broker.join().unwrap();
}

#[cfg(test)]
#[test]
fn disconnect_removes_all_subscriptions() {
    let (tx, rx) = mpsc::channel();
    let broker = thread::spawn(move || run_broker(rx));

    let out_a = new::<OutboundEvent>(4);
    tx.send(BrokerMessage::Register {
        id: 1,
        client_id: "a".into(),
        outbound: out_a.clone(),
    })
    .unwrap();
    tx.send(BrokerMessage::Subscribe {
        id: 1,
        topic: "weather".into(),
    })
    .unwrap();
    tx.send(BrokerMessage::Disconnect { id: 1 }).unwrap();
    tx.send(BrokerMessage::Publish {
        from: 99,
        packet: PublishPacket {
            topic: "weather".into(),
            payload: b"rain".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    })
    .unwrap();

    out_a.close();
    assert!(
        out_a.pop_blocking().is_none(),
        "disconnected client must not receive further publishes"
    );

    drop(tx);
    broker.join().unwrap();
}

#[cfg(test)]
#[test]
fn disconnect_removes_subscriptions_across_multiple_topics() {
    // Regression guard: the Disconnect handler iterates over *all*
    // topic lists to remove the client (broker.rs `for subs in
    // topics.values_mut()`). This test verifies that a client
    // subscribed to three distinct topics receives nothing on any of
    // them after disconnecting — the single-topic case in
    // disconnect_removes_all_subscriptions does not exercise the
    // multi-entry iteration path.
    let (tx, rx) = mpsc::channel();
    let broker = thread::spawn(move || run_broker(rx));

    let out_a = new::<OutboundEvent>(4);
    tx.send(BrokerMessage::Register {
        id: 10,
        client_id: "multi-sub-client".into(),
        outbound: out_a.clone(),
    })
    .unwrap();

    // Subscribe to three separate topics so the Disconnect handler
    // must clean up all three registry entries, not just the first.
    for topic in ["alpha", "beta", "gamma"] {
        tx.send(BrokerMessage::Subscribe {
            id: 10,
            topic: topic.into(),
        })
        .unwrap();
    }

    tx.send(BrokerMessage::Disconnect { id: 10 }).unwrap();

    // Publish to every topic the client was subscribed to — none
    // should be delivered after disconnect.
    for topic in ["alpha", "beta", "gamma"] {
        tx.send(BrokerMessage::Publish {
            from: 99,
            packet: PublishPacket {
                topic: topic.into(),
                payload: b"should-not-arrive".to_vec(),
                qos: 0,
                retain: false,
                packet_id: None,
            },
        })
        .unwrap();
    }

    // drop(tx) before close so the broker thread exits cleanly, then
    // close the queue so pop_blocking() unblocks rather than waiting
    // forever for a message that will never come.
    drop(tx);
    broker.join().unwrap();

    out_a.close();
    assert!(
        out_a.pop_blocking().is_none(),
        "disconnected client must not receive publishes on any of its former topics"
    );
}

/// Stress test: no data loss beyond the documented drop-oldest policy.
///
/// Two scenarios in one test (see Personal_Decisions.md Decision 3B/3C):
///
/// **Scenario A — no-drop load:** 20 clients across 5 topics, 50 messages
/// per topic. 50 < DEFAULT_CLIENT_QUEUE_CAPACITY (128), so zero drops are
/// expected. Each client is subscribed to exactly one topic (round-robin),
/// so each client must receive exactly 50 messages. A deviation means either
/// a message was silently lost or a message was delivered to the wrong client.
///
/// **Scenario B — over-capacity load (drop path):** 1 client subscribed to 1
/// topic, DEFAULT_CLIENT_QUEUE_CAPACITY + 20 = 148 messages published. The
/// queue drops the oldest 20 to stay at capacity. After the broker finishes,
/// draining the queue must yield exactly DEFAULT_CLIENT_QUEUE_CAPACITY = 128
/// items — no more (no duplication), no fewer (no silent extra loss beyond
/// the documented drop-oldest policy).
///
/// Both scenarios use the same drain-after-join strategy: drop(tx) and
/// broker.join() before closing/draining queues, ensuring all Publish
/// messages have been fully processed before we count. This is structurally
/// race-free: the broker is serial, all BrokerMessages are queued before the
/// channel closes, and join() guarantees completion.
#[cfg(test)]
#[test]
fn stress_no_data_loss_beyond_drop_oldest() {
    // ── Scenario A: no-drop load ──────────────────────────────────────────
    // N clients, M topics. Each client subscribes to exactly one topic
    // (client i → topic i % M). The broker must fan out exactly
    // MSGS_PER_TOPIC messages to each subscriber of that topic.
    const N_CLIENTS: usize = 20;
    const N_TOPICS: usize = 5;
    const MSGS_PER_TOPIC: usize = 50; // < DEFAULT_CLIENT_QUEUE_CAPACITY (128)

    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let broker = thread::spawn(move || run_broker(rx));

    // Create per-client queues sized at the real production capacity.
    let queues: Vec<QueueHandle<OutboundEvent>> = (0..N_CLIENTS)
        .map(|_| new::<OutboundEvent>(DEFAULT_CLIENT_QUEUE_CAPACITY))
        .collect();

    // Register all clients.
    for (i, q) in queues.iter().enumerate() {
        let id = (i + 100) as u64; // ids 100..119, no collision with other tests
        tx.send(BrokerMessage::Register {
            id,
            client_id: format!("stress-client-{}", i),
            outbound: q.clone(),
        })
        .unwrap();
    }

    // Subscribe each client to exactly one topic (round-robin).
    // Client i → topic "stress/t{i % N_TOPICS}".
    for i in 0..N_CLIENTS {
        let id = (i + 100) as u64;
        let topic = format!("stress/t{}", i % N_TOPICS);
        tx.send(BrokerMessage::Subscribe { id, topic }).unwrap();
    }

    // Publish MSGS_PER_TOPIC messages to each topic, from a dummy sender.
    for t in 0..N_TOPICS {
        let topic = format!("stress/t{}", t);
        for seq in 0..MSGS_PER_TOPIC {
            tx.send(BrokerMessage::Publish {
                from: 0,
                packet: PublishPacket {
                    topic: topic.clone(),
                    // Encode (topic_index, seq) in the payload for
                    // potential debugging — not verified here, just
                    // counted.
                    payload: format!("t{}:{}", t, seq).into_bytes(),
                    qos: 0,
                    retain: false,
                    packet_id: None,
                },
            })
            .unwrap();
        }
    }

    // Shut the broker down and wait for it to finish processing every
    // message before we touch the queues.
    drop(tx);
    broker.join().unwrap();

    // Drain each client's queue. Since MSGS_PER_TOPIC < CAPACITY, zero
    // drops are expected: every client must receive exactly MSGS_PER_TOPIC
    // messages.
    for (i, q) in queues.into_iter().enumerate() {
        q.close();
        let mut received: usize = 0;
        while q.pop_blocking().is_some() {
            received += 1;
        }
        assert_eq!(
            received,
            MSGS_PER_TOPIC,
            "scenario A: client {} (topic stress/t{}) received {} messages, expected {}",
            i,
            i % N_TOPICS,
            received,
            MSGS_PER_TOPIC,
        );
    }

    // ── Scenario B: over-capacity load (drop path) ────────────────────────
    // One client, one topic, DEFAULT_CLIENT_QUEUE_CAPACITY + 20 publishes.
    // The queue must contain exactly DEFAULT_CLIENT_QUEUE_CAPACITY items
    // after the broker finishes — the excess 20 are dropped (oldest-first),
    // and no further silent loss occurs.
    const OVER: usize = DEFAULT_CLIENT_QUEUE_CAPACITY + 20; // 148

    let (tx2, rx2) = mpsc::channel::<BrokerMessage>();
    let broker2 = thread::spawn(move || run_broker(rx2));

    let single_q = new::<OutboundEvent>(DEFAULT_CLIENT_QUEUE_CAPACITY);
    tx2.send(BrokerMessage::Register {
        id: 200,
        client_id: "stress-overflow".into(),
        outbound: single_q.clone(),
    })
    .unwrap();
    tx2.send(BrokerMessage::Subscribe {
        id: 200,
        topic: "stress/overflow".into(),
    })
    .unwrap();

    for seq in 0..OVER {
        tx2.send(BrokerMessage::Publish {
            from: 0,
            packet: PublishPacket {
                topic: "stress/overflow".into(),
                payload: format!("msg:{}", seq).into_bytes(),
                qos: 0,
                retain: false,
                packet_id: None,
            },
        })
        .unwrap();
    }

    drop(tx2);
    broker2.join().unwrap();

    single_q.close();
    let mut received_b: usize = 0;
    while single_q.pop_blocking().is_some() {
        received_b += 1;
    }
    // The queue holds at most CAPACITY items. The 20 oldest were dropped to
    // make room. No further items should be missing.
    assert_eq!(
        received_b,
        DEFAULT_CLIENT_QUEUE_CAPACITY,
        "scenario B: expected exactly {} items after {}-message overflow (drop-oldest), got {}",
        DEFAULT_CLIENT_QUEUE_CAPACITY,
        OVER,
        received_b,
    );
}

// -----------------------------------------------------------------------
// Sharded-broker tests   (PLAN.md §4 item 3)
// -----------------------------------------------------------------------

/// With num_shards == 1, ShardedBroker must behave identically to a
/// single run_broker call — this confirms the sharding layer introduces
/// no regressions for the degenerate (single-shard) case.
#[cfg(test)]
#[test]
fn sharded_broker_num_shards_1_behaves_like_single_broker() {
    let broker = spawn_sharded_broker(1);

    let out_a = new::<OutboundEvent>(4);
    let out_b = new::<OutboundEvent>(4);

    broker.send(BrokerMessage::Register {
        id: 1,
        client_id: "a".into(),
        outbound: out_a.clone(),
    }).unwrap();
    broker.send(BrokerMessage::Register {
        id: 2,
        client_id: "b".into(),
        outbound: out_b.clone(),
    }).unwrap();
    broker.send(BrokerMessage::Subscribe { id: 1, topic: "news".into() }).unwrap();
    broker.send(BrokerMessage::Subscribe { id: 2, topic: "news".into() }).unwrap();
    broker.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: "news".into(),
            payload: b"headline".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();

    // Both subscribers must receive the publish.
    for (label, q) in [("a", &out_a), ("b", &out_b)] {
        match q.pop_blocking().unwrap_or_else(|| panic!("subscriber {} got nothing", label)) {
            OutboundEvent::Packet(MqttPacket::Publish(p)) => {
                assert_eq!(p.topic, "news");
                assert_eq!(p.payload, b"headline");
            }
            _ => panic!("expected Publish for subscriber {}", label),
        }
    }

    // Disconnect client 1; a publish after that must not reach it.
    broker.send(BrokerMessage::Disconnect { id: 1 }).unwrap();
    broker.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: "news".into(),
            payload: b"late".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();

    // Client 2 should still receive it; client 1 should not.
    match out_b.pop_blocking().expect("client b should still receive after client a disconnect") {
        OutboundEvent::Packet(MqttPacket::Publish(p)) => {
            assert_eq!(p.payload, b"late");
        }
        _ => panic!("expected Publish for client b"),
    }

    // Drain the ShardedBroker (drop all senders).
    drop(broker);
    out_a.close();
    assert!(
        out_a.pop_blocking().is_none(),
        "disconnected client must not receive after disconnect"
    );
}

/// Topics that hash to different shards must be isolated: a subscriber
/// registered *only* via shard X's channel must not receive a publish
/// that goes to shard Y.
///
/// This test drives the two shards' channels directly (bypassing
/// ShardedBroker) so it exercises shard_for_topic + run_broker isolation
/// in the most explicit way possible, without relying on ShardedBroker
/// routing to be correct at the same time.
#[cfg(test)]
#[test]
fn shard_isolation_publish_only_reaches_subscribed_shard() {
    // Spin up exactly 2 shards manually.
    let (tx0, rx0) = mpsc::channel::<BrokerMessage>();
    let (tx1, rx1) = mpsc::channel::<BrokerMessage>();
    let _b0 = thread::spawn(move || run_broker(rx0));
    let _b1 = thread::spawn(move || run_broker(rx1));

    // Find two topics that hash to different shards (shard 0 and shard 1
    // respectively), so this test is not sensitive to which concrete
    // strings happen to hash where.
    let (topic_shard0, topic_shard1) = find_two_topics_on_different_shards(2);

    let out_client = new::<OutboundEvent>(8);

    // Register client only on shard 0, subscribe it to topic_shard0.
    tx0.send(BrokerMessage::Register {
        id: 50,
        client_id: "isolated".into(),
        outbound: out_client.clone(),
    }).unwrap();
    tx0.send(BrokerMessage::Subscribe {
        id: 50,
        topic: topic_shard0.clone(),
    }).unwrap();

    // Publish to topic_shard0 via shard 0 — client MUST receive it.
    tx0.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: topic_shard0.clone(),
            payload: b"for-client".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();

    match out_client.pop_blocking().expect("client should receive publish on its shard") {
        OutboundEvent::Packet(MqttPacket::Publish(p)) => {
            assert_eq!(p.topic, topic_shard0);
        }
        _ => panic!("expected Publish"),
    }

    // Publish to topic_shard1 via shard 1 — client is NOT registered
    // there, so nothing should arrive in its queue.
    // We need shard 1 to process the publish before we check, so we
    // shut shard 1 down and join it first.
    tx1.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: topic_shard1.clone(),
            payload: b"wrong-shard".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();
    drop(tx1);
    _b1.join().unwrap();

    drop(tx0);
    _b0.join().unwrap();

    out_client.close();
    assert!(
        out_client.pop_blocking().is_none(),
        "client registered only on shard 0 must not receive a publish routed to shard 1"
    );
}

/// A client subscribed to topics in two different shards must receive
/// publishes correctly from both — the ShardedBroker's broadcast of
/// Register/Disconnect and single-shard routing of Subscribe/Publish
/// must compose correctly end-to-end.
#[cfg(test)]
#[test]
fn cross_shard_client_receives_from_both_shards() {
    // Use at least 2 shards so the two topics actually land on different
    // shards. We use 4 (NUM_BROKER_SHARDS) for realism.
    const N: usize = 4;
    let broker = spawn_sharded_broker(N);

    // Find two topics on different shards.
    let (topic_a, topic_b) = find_two_topics_on_different_shards(N);
    assert_ne!(
        shard_for_topic(&topic_a, N),
        shard_for_topic(&topic_b, N),
        "test setup: topics must land on different shards"
    );

    let out = new::<OutboundEvent>(16);

    // Register client on *all* shards (ShardedBroker.send broadcasts Register).
    broker.send(BrokerMessage::Register {
        id: 77,
        client_id: "cross-shard-client".into(),
        outbound: out.clone(),
    }).unwrap();

    // Subscribe to one topic on each shard.
    broker.send(BrokerMessage::Subscribe { id: 77, topic: topic_a.clone() }).unwrap();
    broker.send(BrokerMessage::Subscribe { id: 77, topic: topic_b.clone() }).unwrap();

    // Publish one message to each topic.
    broker.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: topic_a.clone(),
            payload: b"from-shard-a".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();
    broker.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: topic_b.clone(),
            payload: b"from-shard-b".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();

    // Collect both deliveries (order is non-deterministic across shards).
    let mut got_a = false;
    let mut got_b = false;
    for _ in 0..2 {
        match out.pop_blocking().expect("cross-shard client must receive 2 publishes") {
            OutboundEvent::Packet(MqttPacket::Publish(p)) => {
                if p.topic == topic_a {
                    assert_eq!(p.payload, b"from-shard-a");
                    got_a = true;
                } else if p.topic == topic_b {
                    assert_eq!(p.payload, b"from-shard-b");
                    got_b = true;
                } else {
                    panic!("unexpected topic: {}", p.topic);
                }
            }
            _ => panic!("expected Publish"),
        }
    }
    assert!(got_a, "must receive publish from topic_a's shard");
    assert!(got_b, "must receive publish from topic_b's shard");

    drop(broker);
    out.close();
    assert!(out.pop_blocking().is_none(), "no extra packets expected");
}

/// Helper: find two topic strings that hash to different shard indices
/// for the given `num_shards`. Uses a simple deterministic search.
#[cfg(test)]
fn find_two_topics_on_different_shards(num_shards: usize) -> (String, String) {
    assert!(num_shards >= 2);
    let mut result: [Option<String>; 2] = [None, None];
    // Iterate short deterministic topic names until we have one topic
    // hashing to shard 0 and one hashing to shard 1.
    for i in 0u64.. {
        let topic = format!("t/{}", i);
        let s = shard_for_topic(&topic, num_shards);
        if s == 0 && result[0].is_none() {
            result[0] = Some(topic);
        } else if s == 1 && result[1].is_none() {
            result[1] = Some(topic);
        }
        if result[0].is_some() && result[1].is_some() {
            break;
        }
    }
    (result[0].take().unwrap(), result[1].take().unwrap())
}

// -----------------------------------------------------------------------
// Wildcard fan-out tests   (PLAN.md §4 item 1 / DECISIONS.md #5)
// -----------------------------------------------------------------------

/// Sanity check (same-shard): a wildcard subscription on a single-shard
/// broker receives a matching publish. This exercises the `run_broker`
/// Publish wildcard pass without any cross-shard complexity.
#[cfg(test)]
#[test]
fn wildcard_subscribe_same_shard_delivers() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let broker = thread::spawn(move || run_broker(rx));

    let out = new::<OutboundEvent>(8);
    tx.send(BrokerMessage::Register {
        id: 1,
        client_id: "wc-client".into(),
        outbound: out.clone(),
    }).unwrap();

    // Subscribe with a '+' wildcard filter.
    tx.send(BrokerMessage::Subscribe {
        id: 1,
        topic: "sensors/+/temp".into(),
    }).unwrap();

    // Publish to a concrete topic that matches the filter.
    tx.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: "sensors/kitchen/temp".into(),
            payload: b"21C".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();

    drop(tx);
    broker.join().unwrap();

    out.close();
    match out.pop_blocking().expect("wildcard subscriber must receive matching publish") {
        OutboundEvent::Packet(MqttPacket::Publish(p)) => {
            assert_eq!(p.topic, "sensors/kitchen/temp");
            assert_eq!(p.payload, b"21C");
        }
        _ => panic!("expected Publish"),
    }
    assert!(out.pop_blocking().is_none(), "no duplicate deliveries");
}

/// A non-matching publish must NOT be delivered to a wildcard subscriber.
/// Guards against an over-broad implementation that delivers everything.
#[cfg(test)]
#[test]
fn wildcard_subscribe_non_matching_not_delivered() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let broker = thread::spawn(move || run_broker(rx));

    let out = new::<OutboundEvent>(8);
    tx.send(BrokerMessage::Register {
        id: 1,
        client_id: "wc-client".into(),
        outbound: out.clone(),
    }).unwrap();
    tx.send(BrokerMessage::Subscribe {
        id: 1,
        topic: "sensors/+/temp".into(),
    }).unwrap();

    // Publish to a topic that does NOT match "sensors/+/temp".
    // "sensors/kitchen/humidity" doesn't end in /temp, so no match.
    tx.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: "sensors/kitchen/humidity".into(),
            payload: b"60%".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();

    drop(tx);
    broker.join().unwrap();

    out.close();
    assert!(
        out.pop_blocking().is_none(),
        "non-matching publish must not be delivered to wildcard subscriber"
    );
}

/// A wildcard subscriber that unsubscribes must not receive subsequent
/// matching publishes.
#[cfg(test)]
#[test]
fn wildcard_unsubscribe_stops_delivery() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let broker = thread::spawn(move || run_broker(rx));

    let out = new::<OutboundEvent>(8);
    tx.send(BrokerMessage::Register {
        id: 1,
        client_id: "wc-client".into(),
        outbound: out.clone(),
    }).unwrap();
    tx.send(BrokerMessage::Subscribe {
        id: 1,
        topic: "sensors/+/temp".into(),
    }).unwrap();
    tx.send(BrokerMessage::Unsubscribe {
        id: 1,
        topic: "sensors/+/temp".into(),
    }).unwrap();
    tx.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: "sensors/kitchen/temp".into(),
            payload: b"21C".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();

    drop(tx);
    broker.join().unwrap();

    out.close();
    assert!(
        out.pop_blocking().is_none(),
        "unsubscribed wildcard client must not receive matching publish"
    );
}

/// Cross-shard wildcard delivery (DECISIONS.md #5 core fix).
///
/// This is the scenario that silently failed before: a wildcard filter
/// (e.g. "sensors/+/temp") hashes to one shard, and a matching concrete
/// publish topic (e.g. "sensors/kitchen/temp") hashes to a *different*
/// shard. Without the broadcast fix, the publish shard has no knowledge
/// of the wildcard subscription, so the subscriber gets nothing.
///
/// We verify this by:
/// 1. Finding a wildcard filter and a matching concrete topic that hash
///    to *different* shards.
/// 2. Subscribing via ShardedBroker (which must broadcast the wildcard).
/// 3. Publishing via ShardedBroker (which routes by concrete topic hash).
/// 4. Confirming delivery.
#[cfg(test)]
#[test]
fn wildcard_cross_shard_delivers() {
    const N: usize = 4;
    let broker = spawn_sharded_broker(N);

    // Find a concrete topic and a matching wildcard filter that hash to
    // DIFFERENT shards under N.
    // Strategy: scan concrete topics of the form "sensors/{i}/temp" and
    // check if the filter "sensors/+/temp" lands on a different shard.
    // The wildcard filter itself hashes to one shard; we need a concrete
    // topic that hashes to a different one.
    let wildcard_filter = "sensors/+/temp";
    let wildcard_shard = shard_for_topic(wildcard_filter, N);

    // Find a concrete topic that matches the filter but hashes to a
    // different shard than the filter itself.
    let concrete_topic: String = (0u64..)
        .map(|i| format!("sensors/{}/temp", i))
        .find(|t| shard_for_topic(t, N) != wildcard_shard)
        .expect("must find a cross-shard concrete topic within a finite search");

    assert_ne!(
        shard_for_topic(&concrete_topic, N),
        wildcard_shard,
        "test setup: concrete topic and wildcard filter must be on different shards"
    );
    // Confirm the concrete topic actually matches the wildcard filter.
    assert!(
        topic_matches_filter(&concrete_topic, wildcard_filter),
        "test setup: {} must match {}", concrete_topic, wildcard_filter,
    );

    let out = new::<OutboundEvent>(8);
    broker.send(BrokerMessage::Register {
        id: 99,
        client_id: "xshard-wc-client".into(),
        outbound: out.clone(),
    }).unwrap();

    // Subscribe with the wildcard filter.
    // ShardedBroker must broadcast this to all shards (including the one
    // that owns the concrete topic's hash bucket).
    broker.send(BrokerMessage::Subscribe {
        id: 99,
        topic: wildcard_filter.to_string(),
    }).unwrap();

    // Publish the concrete matching topic.
    broker.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: concrete_topic.clone(),
            payload: b"cross-shard".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();

    // Deliver must arrive despite the cross-shard mismatch.
    match out.pop_blocking().expect(
        "wildcard subscriber must receive cross-shard matching publish"
    ) {
        OutboundEvent::Packet(MqttPacket::Publish(p)) => {
            assert_eq!(p.topic, concrete_topic);
            assert_eq!(p.payload, b"cross-shard");
        }
        _ => panic!("expected Publish"),
    }

    drop(broker);
    out.close();
    assert!(out.pop_blocking().is_none(), "no duplicate deliveries");
}

/// Cross-shard wildcard: after Unsubscribe, no delivery even when the
/// filter and topic hash to different shards.
#[cfg(test)]
#[test]
fn wildcard_cross_shard_unsubscribe_stops_delivery() {
    const N: usize = 4;
    let broker = spawn_sharded_broker(N);

    let wildcard_filter = "sensors/+/temp";
    let wildcard_shard = shard_for_topic(wildcard_filter, N);
    let concrete_topic: String = (0u64..)
        .map(|i| format!("sensors/{}/temp", i))
        .find(|t| shard_for_topic(t, N) != wildcard_shard)
        .expect("must find a cross-shard concrete topic");

    let out = new::<OutboundEvent>(8);
    broker.send(BrokerMessage::Register {
        id: 98,
        client_id: "xshard-unsub-client".into(),
        outbound: out.clone(),
    }).unwrap();
    broker.send(BrokerMessage::Subscribe {
        id: 98,
        topic: wildcard_filter.to_string(),
    }).unwrap();
    // Unsubscribe — must also broadcast to all shards.
    broker.send(BrokerMessage::Unsubscribe {
        id: 98,
        topic: wildcard_filter.to_string(),
    }).unwrap();
    broker.send(BrokerMessage::Publish {
        from: 0,
        packet: PublishPacket {
            topic: concrete_topic.clone(),
            payload: b"should-not-arrive".to_vec(),
            qos: 0,
            retain: false,
            packet_id: None,
        },
    }).unwrap();

    drop(broker);
    out.close();
    assert!(
        out.pop_blocking().is_none(),
        "after cross-shard wildcard unsubscribe, no publish must be delivered"
    );
}

// -----------------------------------------------------------------------
// Retained-message tests  (PLAN.md §4 item 4 / MQTT §3.3.1.3)
// -----------------------------------------------------------------------

/// Helper: build a PublishPacket with the given topic/payload/retain flag.
#[cfg(test)]
fn make_publish(topic: &str, payload: &[u8], retain: bool) -> PublishPacket {
    PublishPacket {
        topic: topic.to_string(),
        payload: payload.to_vec(),
        qos: 0,
        retain,
        packet_id: None,
    }
}

/// A subscriber that arrives *after* a retained publish on the same topic
/// must immediately receive the retained message (§3.3.1.3).
#[cfg(test)]
#[test]
fn retain_then_subscribe_delivers_retained_message() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let broker_thread = thread::spawn(move || run_broker(rx));

    // Publish a retained message before any subscriber exists.
    tx.send(BrokerMessage::Publish {
        from: 0,
        packet: make_publish("home/temp", b"21C", true),
    }).unwrap();

    // Now a new subscriber arrives.
    let out = new::<OutboundEvent>(8);
    tx.send(BrokerMessage::Register {
        id: 1,
        client_id: "late-sub".into(),
        outbound: out.clone(),
    }).unwrap();
    tx.send(BrokerMessage::Subscribe {
        id: 1,
        topic: "home/temp".into(),
    }).unwrap();

    drop(tx);
    broker_thread.join().unwrap();

    out.close();
    // First event must be the retained replay.
    match out.pop_blocking().expect("subscriber must receive retained message") {
        OutboundEvent::Packet(MqttPacket::Publish(p)) => {
            assert_eq!(p.topic, "home/temp");
            assert_eq!(p.payload, b"21C");
            assert!(p.retain, "delivered retained message must have retain=true (§3.3.1.3)");
        }
        _ => panic!("expected Publish"),
    }
    // No more messages.
    assert!(out.pop_blocking().is_none(), "no extra events expected");
}

/// A second retain on the same topic overwrites the first. A subscriber
/// arriving after both retains must receive only the latest payload.
#[cfg(test)]
#[test]
fn retain_overwrite_new_subscriber_gets_only_latest() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let broker_thread = thread::spawn(move || run_broker(rx));

    // Publish two retained messages for the same topic.
    tx.send(BrokerMessage::Publish {
        from: 0,
        packet: make_publish("sensors/door", b"closed", true),
    }).unwrap();
    tx.send(BrokerMessage::Publish {
        from: 0,
        packet: make_publish("sensors/door", b"open", true),
    }).unwrap();

    let out = new::<OutboundEvent>(8);
    tx.send(BrokerMessage::Register {
        id: 1,
        client_id: "late-sub".into(),
        outbound: out.clone(),
    }).unwrap();
    tx.send(BrokerMessage::Subscribe {
        id: 1,
        topic: "sensors/door".into(),
    }).unwrap();

    drop(tx);
    broker_thread.join().unwrap();

    out.close();
    match out.pop_blocking().expect("must receive retained message") {
        OutboundEvent::Packet(MqttPacket::Publish(p)) => {
            assert_eq!(p.payload, b"open", "only the latest retained payload must be delivered");
            assert!(p.retain);
        }
        _ => panic!("expected Publish"),
    }
    assert!(out.pop_blocking().is_none(), "exactly one retained message expected");
}

/// A retain=true publish with an empty payload clears the retained store
/// for that topic (§3.3.1.3). A subscriber arriving after the clear must
/// receive nothing.
#[cfg(test)]
#[test]
fn retain_empty_payload_clears_stored_message() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let broker_thread = thread::spawn(move || run_broker(rx));

    // Store a retained message.
    tx.send(BrokerMessage::Publish {
        from: 0,
        packet: make_publish("status/light", b"on", true),
    }).unwrap();
    // Clear it with an empty-payload retain.
    tx.send(BrokerMessage::Publish {
        from: 0,
        packet: make_publish("status/light", b"", true),
    }).unwrap();

    let out = new::<OutboundEvent>(8);
    tx.send(BrokerMessage::Register {
        id: 1,
        client_id: "late-sub".into(),
        outbound: out.clone(),
    }).unwrap();
    tx.send(BrokerMessage::Subscribe {
        id: 1,
        topic: "status/light".into(),
    }).unwrap();

    drop(tx);
    broker_thread.join().unwrap();

    out.close();
    // After the clear, new subscribers must receive no retained message.
    assert!(
        out.pop_blocking().is_none(),
        "retained store was cleared; new subscriber must receive nothing"
    );
}

/// A wildcard subscribe after a retained publish on a matching topic must
/// still deliver the retained message (same-shard case: both the exact
/// topic and the wildcard filter route to the same shard).
#[cfg(test)]
#[test]
fn retain_wildcard_subscribe_receives_retained_message() {
    let (tx, rx) = mpsc::channel::<BrokerMessage>();
    let broker_thread = thread::spawn(move || run_broker(rx));

    // Retain a message on "sensors/living/temp".
    tx.send(BrokerMessage::Publish {
        from: 0,
        packet: make_publish("sensors/living/temp", b"22C", true),
    }).unwrap();

    let out = new::<OutboundEvent>(8);
    tx.send(BrokerMessage::Register {
        id: 1,
        client_id: "wildcard-sub".into(),
        outbound: out.clone(),
    }).unwrap();
    // Subscribe with a wildcard that matches the retained topic.
    tx.send(BrokerMessage::Subscribe {
        id: 1,
        topic: "sensors/+/temp".into(),
    }).unwrap();

    drop(tx);
    broker_thread.join().unwrap();

    out.close();
    match out.pop_blocking().expect("wildcard subscriber must receive retained message") {
        OutboundEvent::Packet(MqttPacket::Publish(p)) => {
            assert_eq!(p.topic, "sensors/living/temp");
            assert_eq!(p.payload, b"22C");
            assert!(p.retain, "retained replay must have retain=true");
        }
        _ => panic!("expected Publish"),
    }
    assert!(out.pop_blocking().is_none());
}

/// Cross-shard retained: the retained message's topic hashes to a different
/// shard than the wildcard filter. Because wildcard Subscribe is broadcast
/// to all shards (DECISIONS.md #9), the shard holding the retained message
/// still receives the Subscribe and delivers the replay.
///
/// Ordering guarantee (std-only, no sleep):
/// We first subscribe a "fence" client to the concrete topic (exact match
/// → routes to the same shard as the retained message). We then send the
/// retained Publish (also routed to that shard). We block-wait on the fence
/// client's queue: when it receives the publish, we know that shard has
/// fully processed the BrokerMessage::Publish — meaning the retain store has
/// been updated — before we send the wildcard Subscribe. This is race-free.
#[cfg(test)]
#[test]
fn retain_cross_shard_wildcard_subscribe_receives_retained_message() {
    const N: usize = 4;
    let broker = spawn_sharded_broker(N);

    let wildcard_filter = "sensors/+/temp";
    let wildcard_shard = shard_for_topic(wildcard_filter, N);

    // Find a concrete topic that (a) matches the wildcard and (b) hashes
    // to a DIFFERENT shard — this forces the cross-shard scenario.
    let concrete_topic: String = (0u64..)
        .map(|i| format!("sensors/{}/temp", i))
        .find(|t| shard_for_topic(t, N) != wildcard_shard)
        .expect("must find a cross-shard topic within finite search");

    assert_ne!(
        shard_for_topic(&concrete_topic, N),
        wildcard_shard,
        "test setup: concrete topic and wildcard filter must be on different shards"
    );

    // ── Step 1: Register a "fence" subscriber on the concrete topic. ────
    // Because this is an exact-match subscribe, it routes to the same shard
    // as the concrete topic. When the fence receives the live publish, it
    // proves the retain store on that shard has been updated.
    let fence_out = new::<OutboundEvent>(4);
    broker.send(BrokerMessage::Register {
        id: 51,
        client_id: "fence".into(),
        outbound: fence_out.clone(),
    }).unwrap();
    broker.send(BrokerMessage::Subscribe {
        id: 51,
        topic: concrete_topic.clone(),
    }).unwrap();

    // ── Step 2: Send the retained Publish. ─────────────────────────────
    broker.send(BrokerMessage::Publish {
        from: 0,
        packet: make_publish(&concrete_topic, b"99C", true),
    }).unwrap();

    // ── Step 3: Block until the fence receives the live publish. ────────
    // This guarantees the retain store on that shard is updated before we
    // send the wildcard Subscribe.
    match fence_out.pop_blocking().expect("fence must receive the live publish") {
        OutboundEvent::Packet(MqttPacket::Publish(p)) => {
            assert_eq!(p.topic, concrete_topic, "fence got wrong topic");
        }
        _ => panic!("fence expected Publish"),
    }

    // ── Step 4: Now subscribe with the wildcard — retain is guaranteed. ─
    let out = new::<OutboundEvent>(8);
    broker.send(BrokerMessage::Register {
        id: 50,
        client_id: "xshard-retain-sub".into(),
        outbound: out.clone(),
    }).unwrap();
    broker.send(BrokerMessage::Subscribe {
        id: 50,
        topic: wildcard_filter.to_string(),
    }).unwrap();

    // pop_blocking() BEFORE drop(broker): the queue blocks until the shard
    // processes the Subscribe and delivers the retained replay. This matches
    // the wildcard_cross_shard_delivers pattern.
    match out.pop_blocking().expect(
        "cross-shard wildcard subscriber must receive retained message"
    ) {
        OutboundEvent::Packet(MqttPacket::Publish(p)) => {
            assert_eq!(p.topic, concrete_topic);
            assert_eq!(p.payload, b"99C");
            assert!(p.retain, "cross-shard retained replay must have retain=true");
        }
        _ => panic!("expected Publish"),
    }

    // Shut down shards, then drain to confirm no duplicate deliveries.
    drop(broker);
    out.close();
    assert!(out.pop_blocking().is_none(), "no duplicate deliveries");
}
