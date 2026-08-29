//! The broker actor: owns the topic -> subscriber registry exclusively.
//! All other threads communicate with it only via `BrokerMessage` over an
//! `std::sync::mpsc` channel — see DECISIONS.md #4 for why (no data
//! races on the registry, per-topic publish order preserved, both by
//! construction, not by careful locking).
//!
//! Owner: Role A.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Receiver;

use crate::protocol::{MqttPacket, PublishPacket};
use crate::queue::QueueHandle;

/// Outbound queue capacity per connected client. Tunable — see PLAN.md §3
/// (backpressure: bounded per-client outbound queue, drop-oldest).
pub const DEFAULT_CLIENT_QUEUE_CAPACITY: usize = 128;

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
/// `protocol::encode`.
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
    // topic -> subscribed connection ids. Exact-match only in core scope
    // (no +/# wildcards — see PLAN.md §4 extra #1).
    let mut topics: HashMap<String, Vec<ConnectionId>> = HashMap::new();

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
                let subs = topics.entry(topic).or_default();
                if !subs.contains(&id) {
                    subs.push(id);
                }
            }
            BrokerMessage::Unsubscribe { id, topic } => {
                if let Some(subs) = topics.get_mut(&topic) {
                    subs.retain(|&sid| sid != id);
                }
            }
            BrokerMessage::Publish { from: _, packet } => {
                if let Some(subs) = topics.get(&packet.topic) {
                    for &sid in subs {
                        if let Some(client) = clients.get(&sid) {
                            client
                                .outbound
                                .push(OutboundEvent::Packet(MqttPacket::Publish(packet.clone())));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn publish_fans_out_to_all_subscribers() {
        let (tx, rx) = mpsc::channel();
        let broker = thread::spawn(move || run_broker(rx));

        let out_a = crate::queue::new::<OutboundEvent>(4);
        let out_b = crate::queue::new::<OutboundEvent>(4);

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

    #[test]
    fn disconnect_removes_all_subscriptions() {
        let (tx, rx) = mpsc::channel();
        let broker = thread::spawn(move || run_broker(rx));

        let out_a = crate::queue::new::<OutboundEvent>(4);
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
}
