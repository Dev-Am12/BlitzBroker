# DECISIONS.md — BlitzBroker (MQTT 3.1.1-subset broker, Track C)

Every load-bearing technical or architectural decision made during this build, in the order they were decided. Each entry has the technical reasoning followed by a plain-language recap for quick reading. Past entries are never edited or renumbered — a decision that changes gets a new, later-numbered entry instead.

## Table of Contents

- [1. Concurrency model: single-threaded actor for the topic registry](#1-concurrency-model-single-threaded-actor-for-the-topic-registry)
- [2. TLS / encrypted transport: out of scope](#2-tls--encrypted-transport-out-of-scope)
- [3. Single File (+5) bonus: multi-file development, single-file merge attempted late](#3-single-file-5-bonus-multi-file-development-single-file-merge-attempted-late)
- [4. Package Killer bonus target: `mqtt` / `aedes` (npm)](#4-package-killer-bonus-target-mqtt--aedes-npm)
- [5. Topic wildcard matching: implemented in the protocol layer, not yet wired into the broker](#5-topic-wildcard-matching-implemented-in-the-protocol-layer-not-yet-wired-into-the-broker)
- [6. QoS 1 (PUBACK): parsing + client→broker ack round-trip done; broker→subscriber redelivery/pending-ack tracking is out of scope](#6-qos-1-puback-parsing--clientbroker-ack-round-trip-done-brokersubscriber-redeliverypending-ack-tracking-is-out-of-scope)
- [7. Correction to #6: the broker→client ack direction was missing, found via live verification, now fixed](#7-correction-to-6-the-brokerclient-ack-direction-was-missing-found-via-live-verification-now-fixed)
- [8. Sharded broker: topic registry split into N independent single-threaded shards](#8-sharded-broker-topic-registry-split-into-n-independent-single-threaded-shards)
- [9. Correction to #5: wildcard fan-out is now wired in, and required extending #8's shard-broadcast treatment to wildcard subscriptions](#9-correction-to-5-wildcard-fan-out-is-now-wired-in-and-required-extending-8s-shard-broadcast-treatment-to-wildcard-subscriptions)
- [10. blitzclient: standalone binary with self-contained MQTT encoding — no shared code with the broker](#10-blitzclient-standalone-binary-with-self-contained-mqtt-encoding--no-shared-code-with-the-broker)

---

## 1. Concurrency model: single-threaded actor for the topic registry

**Decision:** One broker thread exclusively owns the topic → subscriber registry; all other threads communicate with it only via `std::sync::mpsc` channels.

**Rationale:** This guarantees no data races on the registry and preserves per-topic publish ordering, by construction — not by careful locking discipline that has to be gotten right across every contributor. The alternative considered was shared-state (`Mutex`/`RwLock`-guarded registry), which allows parallel fan-out but shifts correctness onto lock discipline across four people editing concurrently, which is a harder property to guarantee under time pressure than it looks. Trade-off accepted: the single broker thread is a throughput ceiling under very high concurrent load — documented rather than hidden. A sharded-by-topic-hash upgrade (multiple broker threads, each owning a disjoint set of topics, no cross-shard coordination required) is scoped as an extra-scope item (PLAN.md §4) rather than a redesign, since each topic maps deterministically to exactly one shard.

**In plain terms:** Only one thread is ever allowed to touch the list of who's subscribed to what, so there's no way for two things to corrupt it at the same time — that's true by design, not because everyone was careful. The cost is a ceiling on how much traffic one thread can push through, which we've accepted and written down rather than pretending isn't there.

---

## 2. TLS / encrypted transport: out of scope

**Decision:** No TLS. Plaintext TCP only.

**Rationale:** Rust's `std` has no crypto primitives at all, so a from-scratch TLS implementation carries high correctness risk in this timeframe, and a naive substitute would be actively misleading — worse than no encryption, since it would suggest a security guarantee that isn't real. This is a structural exclusion (not resolvable by more time within this project's constraints), not a time-permitting stretch item, and is documented in README.md as an intentional limitation.

**In plain terms:** We can't build real TLS safely from nothing in this timeframe, and a fake version would be worse than being upfront that there isn't one. MQTT itself doesn't require TLS to function, so this doesn't block the broker from working.

---

## 3. Single File (+5) bonus: multi-file development, single-file merge attempted late

**Decision:** Development proceeds as a normal multi-file Cargo project using Rust's inline `mod {}` blocks for structure. A rehearsal merge into a single file is scheduled before the final hours to de-risk the mechanics ahead of the real submission merge.

**Rationale:** Rust's inline module system means a later consolidation into one file is a largely mechanical process (fixing `use` statements and visibility) rather than a rewrite, so multi-file development throughout doesn't foreclose the bonus. Attempting the merge for the first time in the final hour was rejected as too risky with four people's code involved; a rehearsal checkpoint catches problems while there's still time to fix them.

**In plain terms:** We're building normally, in separate files, because Rust makes it easy to squash that into one file later without much pain — we're just practicing the squash once, well before the deadline, instead of gambling on it working first-try at the very end.

---

## 4. Package Killer bonus target: `mqtt` / `aedes` (npm)

**Decision:** Target the `mqtt`/`aedes` npm packages as the "package killed" claim for the bonus.

**Rationale:** These are the packages most commonly installed for the exact same local pub/sub use case this project addresses from scratch, and both have substantial weekly download counts — making the claim specific and legible to a judge rather than a vague appeal to the broker category in general.

**In plain terms:** These are the actual, real packages a developer would normally `npm install` to get what we built by hand instead.

---

## 5. Topic wildcard matching: implemented in the protocol layer, not yet wired into the broker

**Decision:** Topic-filter validation and the topic/filter matching predicate (`+` single-level, `#` multi-level wildcards, per MQTT 3.1.1 §4.7) live in `protocol.rs`, as pure functions separate from the broker's registry logic.

**Rationale:** `validate_topic_filter` (called from SUBSCRIBE/UNSUBSCRIBE decoding) rejects spec-illegal wildcard placement at parse time. `topic_matches_filter` is implemented iteratively rather than recursively, since both `topic` and `filter` are attacker-influenced strings arriving over the wire, and a recursive implementation would risk stack exhaustion on an adversarial input with many `/` characters. Both are fully unit-tested against the spec's own worked examples. Keeping this logic in the protocol layer (not the broker's registry) keeps wire-format concerns and subscription-matching concerns separately testable.

**Not yet done, verified by a live test rather than assumed:** `broker.rs`'s registry still performs exact-string topic lookup (`HashMap<String, Vec<ConnectionId>>`). A live test — subscribing with `sensors/+/temp` and publishing to `sensors/kitchen/temp` via real mosquitto clients — confirmed the wildcard subscriber currently receives nothing. Swapping the registry's fan-out to use `topic_matches_filter` instead of exact match is the remaining integration step.

**In plain terms:** The code that understands wildcard subscriptions (like `sensors/+/temp`) is written and tested on its own, but the broker's actual message-routing logic doesn't call it yet — so wildcard subscriptions don't work end-to-end yet, confirmed by actually trying it, not just by reading the code.

---

## 6. QoS 1 (PUBACK): parsing + client→broker ack round-trip done; broker→subscriber redelivery/pending-ack tracking is out of scope

**Decision:** Implement `PUBACK` encode/decode and extend `PUBLISH` parsing to accept QoS 1 (packet identifier required, validated non-zero per §2.3.1). Scope this narrowly to "the ack round-trip itself" — not full at-least-once delivery semantics (no DUP-flag redelivery, no broker-side retry-on-timeout, no per-subscriber pending-ack bookkeeping).

**Rationale:** PLAN.md §4 item 2 names the extra scope as "QoS 1 (PUBACK)" without specifying full redelivery guarantees, and implementing genuine at-least-once semantics (tracking in-flight deliveries per subscriber, retry timers, DUP handling) is materially more broker-state-machine work than the remaining hackathon time budget supports well. The ack round-trip itself — a client can publish at QoS 1 and receive a real PUBACK acknowledging it — is the meaningfully-scoped, honestly-doable version of this stretch item.

**Breaking-change note:** adding `MqttPacket::PubAck` made `connection.rs`'s exhaustive `match` on `MqttPacket` fail to compile. A minimal, clearly-commented arm was added there (by Role B, flagged for Role A review — see the `// ROLE B ADDED THIS ARM` comment in `connection.rs`) purely to keep the build green: it treats a received PUBACK as a correct no-op, since there's no pending-ack state anywhere yet to clear.

**Not yet done:** the *other* direction — the broker publishing a QoS 1 message *to* a subscriber and expecting a PUBACK back from them — needs per-subscriber pending-delivery tracking in `broker.rs`'s fan-out logic. That's a real broker-state addition, not a parsing one, and is left as a Role A integration point, same pattern as wildcard fan-out (#5).

**In plain terms:** A client can now publish a QoS 1 message and get a real ack back — that round-trip works and is tested. What's still missing: when the broker forwards a QoS 1 message on to a subscriber, it doesn't yet track whether that subscriber acknowledged it or retry if they didn't.

*(Note, added when #7 was logged: the claim above that the round-trip "works and is tested" turned out to be only half true — see #7. Left unedited here deliberately, per this document's own rule of not rewriting past entries; the correction is recorded as a new entry instead.)*

---

## 7. Correction to #6: the broker→client ack direction was missing, found via live verification, now fixed

**Decision:** Fix `connection.rs`'s `Publish` dispatch arm to send a `PUBACK` back to the publishing client when it receives a QoS 1 `PUBLISH`, per MQTT 3.1.1 §3.3.4.

**Rationale:** Entry #6 only implemented and tested the *client-acking-the-broker* direction (the `PubAck` no-op arm) — it never made the broker itself send an ack when a client publishes at QoS 1, which is the direction §3.3.4 actually requires and the one that matters for a real publisher. Every test written for #6 was a `protocol.rs` encode/decode round-trip; none of them exercised `handle_connection`'s dispatch logic end-to-end, so the gap wasn't caught by the test suite. It was found by running `mosquitto_pub -q 1` against the actual compiled binary — the client hung for the full timeout waiting for an ack that never came. The fix: on receipt of a QoS 1 `PUBLISH`, the broker now pushes a `PUBACK` (echoing the packet identifier) onto that connection's outbound queue immediately, before forwarding to the broker channel. Re-verified the same way the gap was found — `mosquitto_pub -q 1` against the rebuilt binary now completes immediately with a logged `received PUBACK`; QoS 0 was re-checked too, to confirm it still correctly gets no ack and doesn't hang.

**Lesson for how this repo verifies things going forward:** encode/decode unit tests prove the wire format is correct in isolation; they do not prove the broker *behaves* correctly end-to-end. Anything claimed as a working "round-trip" or "flow" should be checked against a real client (mosquitto/paho-mqtt) before being logged as done, not just unit-tested.

**In plain terms:** Entry #6 was half-right — a client publishing at QoS 1 now actually gets acknowledged, verified against a real MQTT client, not just our own tests. What's still missing, same as noted in #6: when the broker forwards a QoS 1 message on to a subscriber, it doesn't yet track whether that subscriber acknowledged it or retry if they didn't.

---

## 8. Sharded broker: topic registry split into N independent single-threaded shards

**Decision:** Replace the single broker actor thread with `NUM_BROKER_SHARDS` (default 4) independent shards, each running the existing `run_broker` loop unmodified over a disjoint subset of topics. A `ShardedBroker` router sits in front: `Subscribe`/`Unsubscribe`/`Publish` for exact-match filters route to exactly one shard, chosen by hashing the topic string (`shard_for_topic`, `std::collections::hash_map::DefaultHasher`, no crate). `Register` and `Disconnect` broadcast to every shard, since a single client may end up with subscriptions spread across more than one.

**Rationale:** This extends #1's design rather than replacing it — each shard is still a single-threaded actor with exclusive ownership of its slice of the registry, so the "no data races, ordering preserved by construction" guarantee from #1 still holds, just per-shard instead of globally. The broadcast treatment for Register/Disconnect was the one real design fork: a shard has no way to know in advance whether a client will ever subscribe to one of its topics, so every shard needs to know about every client's existence and be able to clean up on disconnect regardless. This redundancy (every shard's `clients` map holding every connected client) is intentional and cheap at this project's scale, not a workaround. Existing single-broker tests were kept passing unmodified by keeping `run_broker`'s internal logic untouched and by having `connection.rs`'s `dispatch_packet` accept the send operation through a `BrokerSend` trait, implemented for both a bare `Sender<BrokerMessage>` (what the existing unit tests use) and `ShardedBroker` (what production uses) — so the sharding change didn't require rewriting the connection-level test suite.

**In plain terms:** Instead of one thread handling every topic, there are now several threads, each responsible for its own slice of topics, chosen by hashing the topic name. Messages that don't have a topic (a client connecting or disconnecting) get told to all of them, since any shard might end up needing to know about that client later.

---

## 9. Correction to #5: wildcard fan-out is now wired in, and required extending #8's shard-broadcast treatment to wildcard subscriptions

**Decision:** Wire `topic_matches_filter` into `run_broker`'s `Publish` handling (a wildcard pass alongside the existing exact-match fast path, with de-duplication so a client subscribed both ways doesn't receive a message twice). Additionally — because of #8's sharded design, which didn't exist yet when #5 was written — change `ShardedBroker`'s `Subscribe`/`Unsubscribe` routing so that a wildcard filter (containing `+` or `#`) broadcasts to all shards, the same treatment Register/Disconnect already get, rather than routing by hash like an exact-match filter does.

**Rationale:** #5 correctly identified that `broker.rs` never consulted wildcard filters at all. What #5 didn't anticipate, because sharding didn't exist yet, is that hash-routing a wildcard filter string is itself wrong: `"sensors/+/temp"` and a matching concrete publish like `"sensors/kitchen/temp"` are different strings that generally hash to different shards, so even after wiring in the matching function, a single shard's Publish handler would usually not have the relevant wildcard subscription available to check against. Both halves of the fix were necessary together. Verified independently (not just by the automated suite that was written alongside this fix): three different `+`-filter/topic pairs and one `#`-filter case, deliberately varied to increase the odds of landing on different shards, all delivered correctly through the live broker with real mosquitto clients; a non-matching case correctly delivered nothing.

**In plain terms:** #5 found the first half of the bug (the broker wasn't checking wildcards at all). Fixing it exposed a second half that only existed because of the sharding work in #8 — a wildcard subscription and the message that should match it can easily end up on different shards, so wildcard subscriptions now get told to every shard instead of just one. Tested with real MQTT clients, deliberately across several different topic names to make sure it wasn't just getting lucky on one shard.

---

## 10. blitzclient: standalone binary with self-contained MQTT encoding — no shared code with the broker

**Decision:** `blitzclient` (`src/bin/blitzclient.rs`) implements its own minimal MQTT wire-format encoder and decoder covering only the five packet types it needs: CONNECT, CONNACK, PUBLISH, PUBACK, DISCONNECT. It does not import from `protocol.rs` or any other broker module.

**Rationale:** Already decided by the team before this entry was written — recorded here per AI_GUARDRAILS.md rule 7. The reasoning: blitzclient is a client tool, architecturally separate from the broker binary. Sharing `protocol.rs` would require either (a) turning the project into a library crate (a Cargo.toml change) or (b) using `#[path]` to directly include broker source files into the client binary, which ties the two binaries' build graphs together in a fragile way. A self-contained implementation for five packet types is less than 100 lines of encoding logic, less complexity than either workaround. The client's local implementations are cross-checked against the broker's own tests in the unit test comments (e.g. the CONNACK decoder verified to accept exactly the bytes `encode_connack` in `protocol.rs` produces) — catching spec misunderstandings without a code dependency.

**Technical note:** Cargo auto-discovers files in `src/bin/` and builds each as an independent binary without any `Cargo.toml` addition, so this adds `blitzclient` as a second binary target alongside `blitzbroker` at zero manifest cost.

**In plain terms:** The publish client has its own small copy of the packet-building code it needs, rather than sharing the broker's. They talk the same wire format — checked by test — but aren't linked together in the build.


## 11. Crate structure: pure binary crate without a `lib.rs` (Integration stress tests kept inline)
**Decision:** Keep BlitzBroker as a pure binary crate (only `main.rs` and inline modules) and place the heavy, cross-component stress tests in the `#[cfg(test)]` block of `src/broker.rs`, rather than using Cargo's `tests/` directory.
**Rationale:** Cargo integration tests in `tests/*.rs` can only import from a crate's library target. Adding a `lib.rs` or a `[lib]` section to `Cargo.toml` solely to expose internal structures for testing is a non-trivial structural change that unnecessarily complicates a single-purpose binary. Using `super::*` from within `src/broker.rs`'s test module provides the necessary access to `BrokerMessage`, `run_broker`, and queue internals without altering the crate's architecture.
**In plain terms:** We didn't split the project into a library and a binary just to make a separate `tests/` folder work. We put our heavy stress tests right next to the broker code so they can directly access its internal moving parts.

## 12. Interop tests: standalone external scripts, completely decoupled from `cargo test`
**Decision:** The interop tests (`mosquitto.sh`, `paho_client.py`) are standalone developer tools and are strictly prohibited from being invoked via `std::process::Command` inside a `cargo test`.
**Rationale:** Wiring external tools into `cargo test` creates a hidden environmental dependency. If a machine (like a standard CI runner or a judge's environment) lacks `mosquitto` or `paho-mqtt`, `cargo test` would fail or noisily skip, violating the zero-dependency spirit of the project. Keeping them standalone guarantees that the Rust test suite runs cleanly and is completely self-contained everywhere.
**In plain terms:** Our Rust test suite only tests Rust code and needs zero external tools to pass. The tests that actually talk to real Python or Mosquitto clients are separate scripts you have to run on purpose, preventing `cargo test` from randomly failing just because you don't have Python or Mosquitto installed.
