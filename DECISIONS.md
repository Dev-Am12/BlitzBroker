# DECISIONS.md — BlitzBroker (MQTT 3.1.1-subset broker, Track C)

Every load-bearing technical or architectural decision made during this build, in the order they were decided. Each entry has the technical reasoning followed by a plain-language recap for quick reading. Past entries are never edited or renumbered — a decision that changes gets a new, later-numbered entry instead.

## Table of Contents

- [1. Language: Rust](#1-language-rust)
- [2. Track: C — Web & Network](#2-track-c--web--network)
- [3. Application: MQTT 3.1.1-subset pub/sub broker](#3-application-mqtt-311-subset-pubsub-broker)
- [4. Concurrency model: single-threaded actor for the topic registry](#4-concurrency-model-single-threaded-actor-for-the-topic-registry)
- [5. TLS / encrypted transport: out of scope](#5-tls--encrypted-transport-out-of-scope)
- [6. Single File (+5) bonus: multi-file development, single-file merge attempted late](#6-single-file-5-bonus-multi-file-development-single-file-merge-attempted-late)
- [7. Package Killer bonus target: `mqtt` / `aedes` (npm)](#7-package-killer-bonus-target-mqtt--aedes-npm)
- [8. Topic wildcard matching: implemented in the protocol layer, not yet wired into the broker](#8-topic-wildcard-matching-implemented-in-the-protocol-layer-not-yet-wired-into-the-broker)
- [9. Project name: BlitzBroker (restored entry)](#9-project-name-blitzbroker-restored-entry)

---

## 1. Language: Rust

**Decision:** Rust, `std` only, zero third-party crates.

**Rationale:** The team already had proven Rust experience from a prior hackathon (Port Mortem). More specifically for this event: Rust's standard library is unusually minimal in exactly the areas most projects lean on crates for — no async runtime, no JSON, no HTTP client/server, no crypto. That makes a genuinely zero-dependency build a meaningful engineering claim rather than a technicality — in most other languages (Node, Python, Java) the standard library already covers enough of a "Web & Network" project's needs that avoiding packages is a smaller lift. Alternatives considered: TypeScript/Node (full team fluency, lower execution risk, but a smaller distance travelled from normal practice) and C++ (comparable stdlib minimalism, but only half the team fluent, raising execution risk under a 72-hour clock).

**In plain terms:** Rust's standard library gives us almost nothing for free in exactly the areas this event is testing, so building something real without any packages is a bigger, more honest flex here than in most other languages. We'd also already proven we could ship real Rust fast in a previous hackathon.

---

## 2. Track: C — Web & Network

**Decision:** Track C.

**Rationale:** Best alignment with the team's existing web/backend project experience. Rust's biggest zero-dependency gap (no async runtime, no HTTP/networking crate ecosystem equivalent in `std`) is also most visible in exactly this track — the language's strongest flex and the track's subject matter point the same direction.

**In plain terms:** Networking is where Rust without its usual crates looks most different from normal practice, and it's a track we already had relevant experience in.

---

## 3. Application: MQTT 3.1.1-subset pub/sub broker

**Decision:** Build a broker implementing a real, documented subset of MQTT 3.1.1, rather than a fully custom wire protocol.

**Rationale:** A hand-designed protocol has no external ground truth — correctness can only be self-verified. Implementing a real published subset means genuine off-the-shelf MQTT clients (mosquitto, paho-mqtt) can interoperate with the broker directly, producing an external, provable correctness claim instead of an internal one. This mirrors the verification strategy that worked well in a previous hackathon: external, independently-checkable evidence over self-testing.

**In plain terms:** Instead of inventing our own message format that only we can check, we implemented enough of the real MQTT standard that actual, independent MQTT tools can talk to our broker and prove it works — not just that we say it does.

---

## 4. Concurrency model: single-threaded actor for the topic registry

**Decision:** One broker thread exclusively owns the topic → subscriber registry; all other threads communicate with it only via `std::sync::mpsc` channels.

**Rationale:** This guarantees no data races on the registry and preserves per-topic publish ordering, by construction — not by careful locking discipline that has to be gotten right across every contributor. The alternative considered was shared-state (`Mutex`/`RwLock`-guarded registry), which allows parallel fan-out but shifts correctness onto lock discipline across four people editing concurrently, which is a harder property to guarantee under time pressure than it looks. Trade-off accepted: the single broker thread is a throughput ceiling under very high concurrent load — documented rather than hidden. A sharded-by-topic-hash upgrade (multiple broker threads, each owning a disjoint set of topics, no cross-shard coordination required) is scoped as an extra-scope item (PLAN.md §4) rather than a redesign, since each topic maps deterministically to exactly one shard.

**In plain terms:** Only one thread is ever allowed to touch the list of who's subscribed to what, so there's no way for two things to corrupt it at the same time — that's true by design, not because everyone was careful. The cost is a ceiling on how much traffic one thread can push through, which we've accepted and written down rather than pretending isn't there.

---

## 5. TLS / encrypted transport: out of scope

**Decision:** No TLS. Plaintext TCP only.

**Rationale:** Rust's `std` has no crypto primitives at all, so a from-scratch TLS implementation carries high correctness risk in this timeframe, and a naive substitute would be actively misleading — worse than no encryption, since it would suggest a security guarantee that isn't real. This is a structural exclusion (not resolvable by more time within this project's constraints), not a time-permitting stretch item, and is documented in README.md as an intentional limitation.

**In plain terms:** We can't build real TLS safely from nothing in this timeframe, and a fake version would be worse than being upfront that there isn't one. MQTT itself doesn't require TLS to function, so this doesn't block the broker from working.

---

## 6. Single File (+5) bonus: multi-file development, single-file merge attempted late

**Decision:** Development proceeds as a normal multi-file Cargo project using Rust's inline `mod {}` blocks for structure. A rehearsal merge into a single file is scheduled before the final hours to de-risk the mechanics ahead of the real submission merge.

**Rationale:** Rust's inline module system means a later consolidation into one file is a largely mechanical process (fixing `use` statements and visibility) rather than a rewrite, so multi-file development throughout doesn't foreclose the bonus. Attempting the merge for the first time in the final hour was rejected as too risky with four people's code involved; a rehearsal checkpoint catches problems while there's still time to fix them.

**In plain terms:** We're building normally, in separate files, because Rust makes it easy to squash that into one file later without much pain — we're just practicing the squash once, well before the deadline, instead of gambling on it working first-try at the very end.

---

## 7. Package Killer bonus target: `mqtt` / `aedes` (npm)

**Decision:** Target the `mqtt`/`aedes` npm packages as the "package killed" claim for the bonus.

**Rationale:** These are the packages most commonly installed for the exact same local pub/sub use case this project addresses from scratch, and both have substantial weekly download counts — making the claim specific and legible to a judge rather than a vague appeal to the broker category in general.

**In plain terms:** These are the actual, real packages a developer would normally `npm install` to get what we built by hand instead.

---

## 8. Topic wildcard matching: implemented in the protocol layer, not yet wired into the broker

**Decision:** Topic-filter validation and the topic/filter matching predicate (`+` single-level, `#` multi-level wildcards, per MQTT 3.1.1 §4.7) live in `protocol.rs`, as pure functions separate from the broker's registry logic.

**Rationale:** `validate_topic_filter` (called from SUBSCRIBE/UNSUBSCRIBE decoding) rejects spec-illegal wildcard placement at parse time. `topic_matches_filter` is implemented iteratively rather than recursively, since both `topic` and `filter` are attacker-influenced strings arriving over the wire, and a recursive implementation would risk stack exhaustion on an adversarial input with many `/` characters. Both are fully unit-tested against the spec's own worked examples. Keeping this logic in the protocol layer (not the broker's registry) keeps wire-format concerns and subscription-matching concerns separately testable.

**Not yet done, verified by a live test rather than assumed:** `broker.rs`'s registry still performs exact-string topic lookup (`HashMap<String, Vec<ConnectionId>>`). A live test — subscribing with `sensors/+/temp` and publishing to `sensors/kitchen/temp` via real mosquitto clients — confirmed the wildcard subscriber currently receives nothing. Swapping the registry's fan-out to use `topic_matches_filter` instead of exact match is the remaining integration step.

**In plain terms:** The code that understands wildcard subscriptions (like `sensors/+/temp`) is written and tested on its own, but the broker's actual message-routing logic doesn't call it yet — so wildcard subscriptions don't work end-to-end yet, confirmed by actually trying it, not just by reading the code.

---

## 9. QoS 1 (PUBACK): parsing done; broker→client ack (§3.3.4) was missing on first pass, fixed and live-verified

**Decision:** Implement `PUBACK` encode/decode and extend `PUBLISH` parsing to accept QoS 1 (packet identifier required, validated non-zero per §2.3.1). Scope this narrowly to "the ack round-trip itself" — not full at-least-once delivery semantics (no DUP-flag redelivery, no broker-side retry-on-timeout, no per-subscriber pending-ack bookkeeping).

**Rationale:** PLAN.md §4 item 2 names the extra scope as "QoS 1 (PUBACK)" without specifying full redelivery guarantees, and implementing genuine at-least-once semantics (tracking in-flight deliveries per subscriber, retry timers, DUP handling) is materially more broker-state-machine work than the remaining hackathon time budget supports well. The ack round-trip itself — a client can publish at QoS 1 and receive a real PUBACK acknowledging it — is the meaningfully-scoped, honestly-doable version of this stretch item.

**Correction (caught by Role A's live-verification pass, not by our test suite):** the first version of this work only handled the *client-acking-the-broker* direction (`connection.rs`'s `PubAck` no-op arm) and never made the broker itself send a PUBACK when a client publishes at QoS 1 — the direction §3.3.4 actually requires and the one that matters for a real publisher. All the tests added for this were `protocol.rs` encode/decode round-trips, which can't catch a connection-level behavior gap like this — nothing exercised `handle_connection`'s dispatch logic end-to-end. Role A caught it by running `mosquitto_pub -q 1` against the actual compiled binary and watching it hang for the full timeout. Fixed in `connection.rs`'s `Publish` arm: on receipt of a QoS 1 PUBLISH, the broker now immediately pushes a `PUBACK` (echoing the packet identifier) onto that connection's outbound queue before forwarding to the broker channel. Re-verified the same way Role A found it — `mosquitto_pub -q 1` against the running release binary now completes immediately with a logged `received PUBACK`; QoS 0 re-checked too, to confirm it still correctly gets no ack and doesn't hang either.

**Lesson for how this repo verifies things going forward:** encode/decode unit tests prove the wire format is correct in isolation; they do not prove the broker *behaves* correctly end-to-end. Anything claimed as a working "round-trip" or "flow" should be checked against a real client (mosquitto/paho-mqtt) before being logged as done, not just unit-tested.

**Regression coverage added:** three tests in `connection.rs` exercise `dispatch_packet` directly (real `mpsc` channel + real `queue::QueueHandle`, no socket needed — same technique as the existing SUBACK/UNSUBACK tests) so this exact class of bug can't silently return: a QoS 1 PUBLISH must produce a matching PUBACK on the publisher's own queue, a QoS 0 PUBLISH must produce none, and multiple QoS 1 PUBLISHes must each get correctly-matched acks. Confirmed these tests actually catch the bug — not just that they pass — by temporarily reverting the fix and watching the core test fail before restoring it.

**Breaking-change note:** adding `MqttPacket::PubAck` made `connection.rs`'s exhaustive `match` on `MqttPacket` fail to compile. A minimal, clearly-commented arm was added there (by Role B, flagged for Role A review — see the `// ROLE B ADDED THIS ARM` comment in `connection.rs`) purely to keep the build green: it treats a received PUBACK (client acking the broker) as a correct no-op, since there's no pending-ack state anywhere yet to clear. The *separate* broker→client ack fix above lives in the `Publish` arm, not this one.

**Not yet done:** the broker publishing a QoS 1 message *to* a subscriber and expecting a PUBACK back from *them* still needs per-subscriber pending-delivery tracking in `broker.rs`'s fan-out logic. That's a real broker-state addition, not a parsing/dispatch one, and is left as a Role A integration point, same pattern as wildcard fan-out (#8).

**In plain terms:** a client publishing a QoS 1 message to this broker now actually gets acknowledged — verified against a real MQTT client, not just our own tests. What's still missing: when the broker forwards a QoS 1 message on to a subscriber, it doesn't yet track whether that subscriber acknowledged it or retry if they didn't.