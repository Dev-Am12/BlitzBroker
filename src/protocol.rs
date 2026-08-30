//! MQTT 3.1.1 wire protocol — packet types and (de)serialization.
//!
//! Owner: Role B. The enum/struct shapes below are the stable contract
//! Role A's connection-handling code (connection.rs) is built against —
//! treat them as fixed unless a change is logged in DECISIONS.md, since
//! connection.rs depends on these exact names and fields. Fill in the
//! `todo!()` bodies of `decode`/`encode`/the remaining-length functions.
//!
//! Spec: MQTT Version 3.1.1, OASIS Standard.
//! Core scope (PLAN.md §3): CONNECT/CONNACK, SUBSCRIBE/SUBACK,
//! UNSUBSCRIBE/UNSUBACK, PUBLISH (QoS 0 only), PINGREQ/PINGRESP,
//! DISCONNECT. Extras (§4): wildcards, QoS 1, retained/will messages.

use crate::error::ProtocolError;

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

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;

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

    #[test]
    fn remaining_length_roundtrip_single_byte() {
        // Spec example: 64 encodes as a single byte 0x40.
        let encoded = encode_remaining_length(64);
        assert_eq!(encoded, vec![0x40]);
        assert_eq!(decode_remaining_length(&encoded), Ok((64, 1)));
    }

    #[test]
    fn remaining_length_roundtrip_two_bytes() {
        // Spec example: 321 encodes as 0xC1 0x02.
        let encoded = encode_remaining_length(321);
        assert_eq!(encoded, vec![0xC1, 0x02]);
        assert_eq!(decode_remaining_length(&encoded), Ok((321, 2)));
    }

    #[test]
    fn remaining_length_max_value_four_bytes() {
        // Spec max: 268,435,455 (0xFF 0xFF 0xFF 0x7F).
        let encoded = encode_remaining_length(268_435_455);
        assert_eq!(encoded, vec![0xFF, 0xFF, 0xFF, 0x7F]);
        assert_eq!(decode_remaining_length(&encoded), Ok((268_435_455, 4)));
    }

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

    #[test]
    fn decode_empty_buffer_is_incomplete_not_panic() {
        assert!(matches!(decode(&[]), Err(ProtocolError::Incomplete)));
    }

    #[test]
    fn decode_unknown_packet_type_is_rejected() {
        // Packet type nibble 0 is reserved / unused in MQTT 3.1.1.
        let buf = [0x00u8, 0x00];
        assert!(matches!(
            decode(&buf),
            Err(ProtocolError::UnknownPacketType(0))
        ));
    }

    #[test]
    fn decode_truncated_after_fixed_header_is_incomplete() {
        // Claims 10 remaining bytes but buffer only has the header.
        let buf = [(PT_PINGREQ << 4), 10];
        assert!(matches!(decode(&buf), Err(ProtocolError::Incomplete)));
    }

    #[test]
    fn decode_pingreq_roundtrip() {
        let bytes = encode(&MqttPacket::PingReq);
        let (packet, consumed) = decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert!(matches!(packet, MqttPacket::PingReq));
    }

    #[test]
    fn decode_pingreq_rejects_nonzero_remaining_length() {
        // §3.13.1: PINGREQ has no variable header or payload.
        let buf = [(PT_PINGREQ << 4), 0x01, 0xAB];
        assert!(matches!(decode(&buf), Err(ProtocolError::MalformedPayload(_))));
    }

    // --- UTF-8 string field, §1.5.3 ---

    #[test]
    fn utf8_string_rejects_truncated_length_prefix() {
        assert!(decode_utf8_string(&[0x00]).is_err());
    }

    #[test]
    fn utf8_string_rejects_length_exceeding_buffer() {
        // Claims 100 bytes follow but only 2 are present.
        let buf = [0x00, 0x64, b'h', b'i'];
        assert!(decode_utf8_string(&buf).is_err());
    }

    #[test]
    fn utf8_string_rejects_invalid_utf8_bytes() {
        let buf = [0x00, 0x02, 0xFF, 0xFE];
        assert!(decode_utf8_string(&buf).is_err());
    }

    // --- CONNECT, §3.1 ---

    fn sample_connect() -> ConnectPacket {
        ConnectPacket {
            protocol_name: "MQTT".to_string(),
            protocol_level: 4,
            clean_session: true,
            keep_alive_secs: 60,
            client_id: "test-client".to_string(),
        }
    }

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

    #[test]
    fn subscribe_rejects_empty_topic_list() {
        let mut body = Vec::new();
        body.extend_from_slice(&1u16.to_be_bytes());
        let bytes = prepend_fixed_header(PT_SUBSCRIBE, 0b0010, body);
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

    #[test]
    fn subscribe_rejects_invalid_qos() {
        let mut body = Vec::new();
        body.extend_from_slice(&1u16.to_be_bytes());
        encode_utf8_string("a", &mut body);
        body.push(3); // invalid QoS
        let bytes = prepend_fixed_header(PT_SUBSCRIBE, 0b0010, body);
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

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

    #[test]
    fn publish_rejects_qos2_reserved_value() {
        let mut body = Vec::new();
        encode_utf8_string("t", &mut body);
        // flags: QoS bits = 11 (3), the reserved/invalid combination.
        let bytes = prepend_fixed_header(PT_PUBLISH, 0b0110, body);
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

    #[test]
    fn publish_rejects_qos2_as_unsupported() {
        let mut body = Vec::new();
        encode_utf8_string("t", &mut body);
        body.extend_from_slice(&1u16.to_be_bytes()); // packet id, if it were QoS2
        let bytes = prepend_fixed_header(PT_PUBLISH, 0b0100, body); // QoS bits = 10
        assert!(matches!(decode(&bytes), Err(ProtocolError::UnsupportedFeature(_))));
    }

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

    #[test]
    fn publish_qos1_rejects_truncated_packet_identifier() {
        let mut body = Vec::new();
        encode_utf8_string("t", &mut body);
        // Only 1 byte of what should be a 2-byte packet identifier.
        body.push(0x00);
        let bytes = prepend_fixed_header(PT_PUBLISH, 0b0010, body); // QoS bits = 01
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

    #[test]
    fn publish_qos1_rejects_zero_packet_identifier() {
        let mut body = Vec::new();
        encode_utf8_string("t", &mut body);
        body.extend_from_slice(&0u16.to_be_bytes()); // packet id 0 — illegal
        let bytes = prepend_fixed_header(PT_PUBLISH, 0b0010, body);
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

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

    #[test]
    fn puback_rejects_wrong_body_length() {
        // 3 bytes instead of the required exactly-2.
        let bytes = prepend_fixed_header(PT_PUBACK, 0, vec![0x00, 0x01, 0x02]);
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

    #[test]
    fn puback_rejects_zero_packet_identifier() {
        let bytes = prepend_fixed_header(PT_PUBACK, 0, 0u16.to_be_bytes().to_vec());
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

    #[test]
    fn puback_rejects_nonzero_flags() {
        // Fixed header flags for PUBACK are reserved and must be 0
        // (§3.4.1) — construct one with a nonzero flag nibble by hand.
        let mut bytes = prepend_fixed_header(PT_PUBACK, 0, 2u16.to_be_bytes().to_vec());
        bytes[0] |= 0b0001; // set a reserved flag bit
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

    #[test]
    fn publish_rejects_empty_topic_name() {
        let mut body = Vec::new();
        encode_utf8_string("", &mut body);
        let bytes = prepend_fixed_header(PT_PUBLISH, 0, body);
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

    #[test]
    fn publish_rejects_wildcard_in_topic_name() {
        let mut body = Vec::new();
        encode_utf8_string("a/+/b", &mut body);
        let bytes = prepend_fixed_header(PT_PUBLISH, 0, body);
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

    // --- Topic wildcards, §4.7 ---

    #[test]
    fn validate_filter_accepts_plain_topic() {
        assert!(validate_topic_filter("sport/tennis/player1").is_ok());
    }

    #[test]
    fn validate_filter_accepts_hash_alone() {
        assert!(validate_topic_filter("#").is_ok());
    }

    #[test]
    fn validate_filter_accepts_hash_as_last_level() {
        assert!(validate_topic_filter("sport/tennis/player1/#").is_ok());
    }

    #[test]
    fn validate_filter_rejects_hash_not_last() {
        assert!(matches!(
            validate_topic_filter("sport/#/player1"),
            Err(ProtocolError::MalformedPayload(_))
        ));
    }

    #[test]
    fn validate_filter_rejects_hash_not_alone_in_level() {
        assert!(matches!(
            validate_topic_filter("sport/tennis#"),
            Err(ProtocolError::MalformedPayload(_))
        ));
    }

    #[test]
    fn validate_filter_accepts_plus_at_any_level() {
        assert!(validate_topic_filter("sport/+/player1").is_ok());
        assert!(validate_topic_filter("+/+").is_ok());
        assert!(validate_topic_filter("+").is_ok());
    }

    #[test]
    fn validate_filter_rejects_plus_not_alone_in_level() {
        assert!(matches!(
            validate_topic_filter("sport/+aa/player1"),
            Err(ProtocolError::MalformedPayload(_))
        ));
    }

    #[test]
    fn validate_filter_rejects_empty_filter() {
        assert!(matches!(
            validate_topic_filter(""),
            Err(ProtocolError::MalformedPayload(_))
        ));
    }

    #[test]
    fn matches_exact_topic() {
        assert!(topic_matches_filter("sport/tennis/player1", "sport/tennis/player1"));
        assert!(!topic_matches_filter("sport/tennis/player1", "sport/tennis/player2"));
    }

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

    #[test]
    fn matches_plus_does_not_match_missing_level() {
        // "sport/+" requires a second level to exist; "sport" alone
        // must not match.
        assert!(!topic_matches_filter("sport", "sport/+"));
    }

    #[test]
    fn matches_leading_slash_topics() {
        // "+/+" matches "/finance" (an empty first level, per the
        // spec's own worked example in §4.7.1.3).
        assert!(topic_matches_filter("/finance", "+/+"));
    }

    #[test]
    fn matches_never_panics_on_adversarial_slash_counts() {
        // Regression-style guard: matching must stay well-behaved even
        // on a topic/filter with a large number of levels, since both
        // strings are attacker-influenced (arrive over the wire).
        let many_slashes = "a/".repeat(10_000);
        assert!(!topic_matches_filter(&many_slashes, "x/y/z"));
        assert!(topic_matches_filter(&many_slashes, "#"));
    }

    #[test]
    fn subscribe_rejects_invalid_wildcard_filter() {
        let mut body = Vec::new();
        body.extend_from_slice(&1u16.to_be_bytes());
        encode_utf8_string("sport/tennis#", &mut body); // invalid: # not alone
        body.push(0);
        let bytes = prepend_fixed_header(PT_SUBSCRIBE, 0b0010, body);
        assert!(matches!(decode(&bytes), Err(ProtocolError::MalformedPayload(_))));
    }

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
}