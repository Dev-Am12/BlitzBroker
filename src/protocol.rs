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
/// identifier per spec).
#[derive(Debug, Clone)]
pub struct PublishPacket {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub packet_id: Option<u16>,
}

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
    let _ = buf;
    todo!(
        "Role B: implement per MQTT 3.1.1 §2 (fixed header + remaining \
         length) and the per-packet-type sections cited on each struct \
         above. Start with decode_remaining_length below — everything \
         else builds on it."
    )
}

/// Encode a packet to its wire representation.
pub fn encode(packet: &MqttPacket) -> Vec<u8> {
    let _ = packet;
    todo!("Role B: implement per MQTT 3.1.1 §2 and per-packet-type sections")
}

/// Decode the MQTT "Remaining Length" variable-length field (§2.2.3)
/// starting at `buf`. Returns `(value, bytes_consumed)` on success.
/// Max 4 bytes, 7 data bits per byte, continuation bit in the MSB.
pub fn decode_remaining_length(buf: &[u8]) -> Result<(u32, usize), ProtocolError> {
    let _ = buf;
    todo!("Role B: implement per MQTT 3.1.1 §2.2.3")
}

/// Encode a value as the MQTT "Remaining Length" variable-length field.
pub fn encode_remaining_length(len: u32) -> Vec<u8> {
    let _ = len;
    todo!("Role B: implement per MQTT 3.1.1 §2.2.3")
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
}
