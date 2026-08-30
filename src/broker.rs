//! The broker actor: owns the topic -> subscriber registry exclusively.
//! All other threads communicate with it only via `BrokerMessage` over an
//! `std::sync::mpsc` channel — see DECISIONS.md #1 for why (no data
//! races on the registry, per-topic publish order preserved, both by
//! construction, not by careful locking).
//!
//! Sharding (PLAN.md §4 item 3 / DECISIONS.md #1 upgrade path):
//! `ShardedBroker` runs N independent `run_broker` threads, each owning a
//! disjoint subset of topics. A topic deterministically maps to one shard
//! via `shard_for_topic` (hash mod N), so no cross-shard coordination is
//! ever needed. Register/Disconnect are broadcast to all shards because
//! every shard must know about every client (a client may later subscribe
//! to a topic owned by any shard). This redundancy is intentional and
//! cheap at this scale — see DECISIONS.md #1 for the full reasoning.
//!
//! Wildcard subscriptions (PLAN.md §4 item 1 / DECISIONS.md #5):
//! A subscription filter containing '+' or '#' is broadcast to *all*
//! shards (same broadcast rule as Register/Disconnect). This is necessary
//! because a publish to "sensors/kitchen/temp" hashes to the shard for
//! that literal string, not to the shard that received "sensors/+/temp".
//! Every shard therefore holds every wildcard filter and checks them
//! against incoming publishes via `protocol::topic_matches_filter`.
//! Exact-match filters (no wildcards) continue to route to a single shard
//! by hash, unchanged — the fast path is preserved.
//!
//! Owner: Role A.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

use crate::protocol::{topic_matches_filter, MqttPacket, PublishPacket};
use crate::queue::QueueHandle;

/// Return true if `filter` contains MQTT wildcard characters ('+' or '#').
/// Used to decide whether a Subscribe/Unsubscribe must be broadcast to
/// all shards (wildcard) or routed to a single shard by hash (exact match).
/// The filter's wildcard syntax is already validated upstream by
/// `protocol::validate_topic_filter` before the `BrokerMessage` is built.
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

    // -----------------------------------------------------------------------
    // Sharded-broker tests   (PLAN.md §4 item 3)
    // -----------------------------------------------------------------------

    /// With num_shards == 1, ShardedBroker must behave identically to a
    /// single run_broker call — this confirms the sharding layer introduces
    /// no regressions for the degenerate (single-shard) case.
    #[test]
    fn sharded_broker_num_shards_1_behaves_like_single_broker() {
        let broker = spawn_sharded_broker(1);

        let out_a = crate::queue::new::<OutboundEvent>(4);
        let out_b = crate::queue::new::<OutboundEvent>(4);

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

        let out_client = crate::queue::new::<OutboundEvent>(8);

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

        let out = crate::queue::new::<OutboundEvent>(16);

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
    #[test]
    fn wildcard_subscribe_same_shard_delivers() {
        let (tx, rx) = mpsc::channel::<BrokerMessage>();
        let broker = thread::spawn(move || run_broker(rx));

        let out = crate::queue::new::<OutboundEvent>(8);
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
    #[test]
    fn wildcard_subscribe_non_matching_not_delivered() {
        let (tx, rx) = mpsc::channel::<BrokerMessage>();
        let broker = thread::spawn(move || run_broker(rx));

        let out = crate::queue::new::<OutboundEvent>(8);
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
    #[test]
    fn wildcard_unsubscribe_stops_delivery() {
        let (tx, rx) = mpsc::channel::<BrokerMessage>();
        let broker = thread::spawn(move || run_broker(rx));

        let out = crate::queue::new::<OutboundEvent>(8);
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
            crate::protocol::topic_matches_filter(&concrete_topic, wildcard_filter),
            "test setup: {} must match {}", concrete_topic, wildcard_filter,
        );

        let out = crate::queue::new::<OutboundEvent>(8);
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

        let out = crate::queue::new::<OutboundEvent>(8);
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
}
