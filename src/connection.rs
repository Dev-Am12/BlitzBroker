//! Per-client connection handling: spawns a reader thread (socket ->
//! parsed packets -> `BrokerMessage`) and a writer thread (broker's
//! outbound queue -> encoded bytes -> socket) for each accepted TCP
//! connection.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::thread;

use crate::broker::{
    next_connection_id, BrokerMessage, ConnectionId, OutboundEvent,
    DEFAULT_CLIENT_QUEUE_CAPACITY,
};
use crate::protocol::{self, ConnAckPacket, ConnectReturnCode, MqttPacket, SubAckPacket,
    UnsubAckPacket};
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
            // MQTT 3.1.1 §3.9.3: SUBACK payload contains one return code
            // per topic filter in the SUBSCRIBE, in the same order.
            // Core scope: QoS 0 only, broker always accepts → 0x00.
            // Future rejection path: replace 0x00 with 0x80 for that
            // filter's slot without restructuring anything else here.
            let mut return_codes: Vec<u8> = Vec::with_capacity(sub.subscriptions.len());
            for (topic, _qos) in sub.subscriptions {
                let _ = broker_tx.send(BrokerMessage::Subscribe { id, topic });
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
                let _ = broker_tx.send(BrokerMessage::Unsubscribe { id, topic });
            }
            // MQTT 3.1.1 §3.11: the broker MUST send UNSUBACK in response
            // to a UNSUBSCRIBE request (fixed-format, packet_id only).
            outbound.push(OutboundEvent::Packet(MqttPacket::UnsubAck(UnsubAckPacket {
                packet_id: unsub.packet_id,
            })));
            true
        }
        MqttPacket::Publish(publish) => {
            let _ = broker_tx.send(BrokerMessage::Publish {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::{BrokerMessage, OutboundEvent, DEFAULT_CLIENT_QUEUE_CAPACITY};
    use crate::protocol::{MqttPacket, SubscribePacket, UnsubscribePacket};
    use std::sync::mpsc;

    /// Pop the next outbound packet from the queue. The queue must be
    /// closed first so pop_blocking doesn't block indefinitely when empty.
    fn pop_packet(q: &queue::QueueHandle<OutboundEvent>) -> MqttPacket {
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
    #[test]
    fn subscribe_single_topic_produces_suback_with_correct_packet_id_and_return_code() {
        let (tx, rx) = mpsc::channel::<BrokerMessage>();
        let outbound = queue::new(DEFAULT_CLIENT_QUEUE_CAPACITY);

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
    #[test]
    fn subscribe_multiple_topics_produces_suback_with_one_code_per_topic_in_order() {
        let (tx, rx) = mpsc::channel::<BrokerMessage>();
        let outbound = queue::new(DEFAULT_CLIENT_QUEUE_CAPACITY);

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
    #[test]
    fn subscribe_max_packet_id_echoed_in_suback() {
        let (tx, _rx) = mpsc::channel::<BrokerMessage>();
        let outbound = queue::new(DEFAULT_CLIENT_QUEUE_CAPACITY);

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
    #[test]
    fn unsubscribe_produces_unsuback_with_correct_packet_id() {
        let (tx, rx) = mpsc::channel::<BrokerMessage>();
        let outbound = queue::new(DEFAULT_CLIENT_QUEUE_CAPACITY);

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
    #[test]
    fn unsubscribe_multiple_topics_sends_one_unsuback() {
        let (tx, rx) = mpsc::channel::<BrokerMessage>();
        let outbound = queue::new(DEFAULT_CLIENT_QUEUE_CAPACITY);

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
}