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

        let out_a = crate::queue::new::<OutboundEvent>(4);
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
        let queues: Vec<crate::queue::QueueHandle<OutboundEvent>> = (0..N_CLIENTS)
            .map(|_| crate::queue::new::<OutboundEvent>(DEFAULT_CLIENT_QUEUE_CAPACITY))
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

        let single_q = crate::queue::new::<OutboundEvent>(DEFAULT_CLIENT_QUEUE_CAPACITY);
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
}
