# DECISIONS.md — BlitzBroker

Numbered log of load-bearing project decisions, in the order they were made. Add new entries at the end as further decisions are made. Do not renumber or edit past entries — supersede a decision with a new, later-numbered entry if it changes.

1. **Language: Rust.** `std`'s minimal built-in surface (no async runtime, no JSON, no HTTP, no crypto) makes a genuinely zero-dependency build a meaningful engineering claim.

2. **Track: C — Web & Network.**

3. **Application: MQTT 3.1.1-subset pub/sub broker.** A fully custom protocol was rejected in favor of a real, published subset of MQTT, so the broker can be interoperability-tested against genuine off-the-shelf MQTT clients rather than relying on self-verification alone.

4. **Concurrency model: single-threaded actor for the topic registry.** One broker thread exclusively owns the topic → subscriber registry; all other threads communicate with it only via `std::sync::mpsc` channels. This guarantees no data races on the registry and preserves per-topic publish ordering, by construction. Trade-off: the single broker thread is a throughput ceiling under very high concurrent load — accepted for this scope and documented rather than hidden. A sharded-by-topic-hash upgrade (multiple broker threads, each owning a disjoint set of topics, no cross-shard coordination required) is listed as an extra-scope item in PLAN.md §4 if time permits, rather than requiring a redesign.

5. **TLS / encrypted transport: out of scope.** Rust's `std` has no crypto primitives at all, so a from-scratch TLS implementation carries high correctness risk in this timeframe, and a naive substitute would be actively misleading. This is a structural exclusion, not a time-permitting stretch item, and is documented in README.md as an intentional limitation.

6. **Single File (+5) bonus: multi-file development, single-file merge attempted late.** Development proceeds as a normal multi-file Cargo project using Rust's inline `mod {}` blocks for structure. A rehearsal merge into a single file is scheduled before the final hours (see PLAN.md §6) to de-risk the mechanics ahead of the real submission merge.

7. **Package Killer bonus target: `mqtt` / `aedes` (npm).** These are the packages most commonly installed for the same local pub/sub use case this project addresses from scratch.

8. **Topic wildcard matching (extra scope, PLAN.md §4 item 1): filter validation and the topic/filter matching predicate live in `protocol.rs` (Role B), not `broker.rs`.** `validate_topic_filter` (called from `decode_subscribe`/`decode_unsubscribe`) rejects spec-illegal wildcard placement at parse time. `topic_matches_filter` is a standalone, iterative (not recursive — see its doc comment for why) pure function implementing §4.7.1's matching rules, fully unit-tested against the spec's own worked examples. **Not yet done:** `broker.rs`'s registry still does exact-string topic lookup (`HashMap<String, Vec<ConnectionId>>`) — swapping fan-out to use `topic_matches_filter` instead of exact match is a Role A integration step, intentionally left for them to review/wire in rather than editing their file directly.