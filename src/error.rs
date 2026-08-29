//! Shared error types used across the broker.

use std::fmt;

/// Errors that can occur while decoding an MQTT packet from raw bytes.
///
/// Every variant here must correspond to a documented rejection path —
/// see AI_GUARDRAILS.md rule 3: parsers must never panic on untrusted
/// input, they must return one of these instead.
#[derive(Debug)]
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
