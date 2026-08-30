# Personal Log — Role B: Protocol & Packet Parsing

**Project:** BlitzBroker
**Owner:** Member B

## Scope (from PLAN.md §5)
Fixed/variable header parsing, remaining-length encoding/decoding, all packet-type encode/decode logic, malformed-input rejection paths.

## Task queue
- [x] Remaining-length variable encoding/decoding (MQTT spec)
- [x] Fixed header parsing (packet type, flags)
- [x] CONNECT / CONNACK
- [x] SUBSCRIBE / SUBACK, UNSUBSCRIBE / UNSUBACK
- [x] PUBLISH (QoS 0)
- [x] PINGREQ / PINGRESP, DISCONNECT
- [x] Malformed/truncated/oversized-input rejection paths for every packet type (cite spec section for each)
- [x] (stretch) Topic wildcards (+, #) — validation + matching predicate done; broker fan-out integration is a Role A step, see DECISIONS.md #8
- [x] (stretch) QoS 1 (PUBACK) — ack round-trip done; broker-side redelivery/pending-ack tracking out of scope, see DECISIONS.md #9

## Log
_Add dated entries below as you go — what you did, decisions made, blockers hit._

### 2026-08-30

Implemented `decode()`/`encode()` and all the per-packet-type functions in `protocol.rs` that Role A left as `todo!()` stubs. Kept every struct/enum shape exactly as defined (per the file's own header comment — connection.rs is built against them as a fixed contract) — no changes to types, only filled in the bodies.

**What's done, per MQTT 3.1.1 spec section:**
- `decode_remaining_length` / `encode_remaining_length` (§2.2.3) — 1–4 byte variable-length codec.
- Fixed header parsing (§2.2.1) — packet type nibble + flags nibble.
- CONNECT/CONNACK (§3.1/§3.2): protocol name/level validated, clean-session flag, keep-alive, client ID. Will/username/password flags are rejected with `UnsupportedFeature` rather than silently dropped, since core scope doesn't handle them (see `ConnectPacket` doc comment) and silently ignoring them would misrepresent what the client asked for.
- SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK (§3.8–§3.11): topic-filter list parsing, per-subscription QoS validated (0–2 accepted at parse time even though only QoS 0 delivery exists yet — rejecting QoS 1/2 *subscriptions* outright felt like the wrong layer; that's a broker delivery-behavior decision, not a parsing one).
- PUBLISH (§3.3): QoS 0 only per PLAN.md §3 — QoS 1/2 rejected as `UnsupportedFeature`/`MalformedPayload` respectively. Also rejects empty topic names and wildcard characters (`+`/`#`) in a topic *name* per §4.7.1/§4.7.3 (wildcards are only legal in subscription filters).
- PINGREQ/PINGRESP, DISCONNECT (§3.13/§3.14): zero-remaining-length enforced.
- UTF-8 string field helper (§1.5.3) shared by every packet type that has string fields.

**Decisions made:**
- Added `PartialEq, Eq` derive to `ProtocolError` in `error.rs` (previously just `Debug`) — needed for `assert_eq!` against `Result<_, ProtocolError>` in tests. Additive only, doesn't change any existing behavior.
- Broker→client packet types (CONNACK/SUBACK/UNSUBACK/PINGRESP) received *from* a client are recognized but rejected via `UnsupportedFeature`, not `UnknownPacketType` — they're valid MQTT packet types, just never legal in that direction.

**Bug found and fixed via a stdlib-only fuzz smoke test** (`decode_never_panics_on_random_bytes`, 20k random byte buffers, no crate — cheap xorshift PRNG): `decode_remaining_length` could overflow on a malformed 5-continuation-byte input, because the original "5th byte" bounds check ran *after* the arithmetic that could already overflow. This was a real AI_GUARDRAILS.md rule-3 violation (panic on untrusted input). Fixed by checking byte count (`i >= 4`) *before* touching the byte, instead of checking the multiplier after using it. Test is kept permanently in the suite as a regression check.

**Status:** 34/34 tests passing, clean `cargo build` and `cargo build --release`, no `unwrap`/`expect`/panics anywhere in the decode path. Core scope for Role B is functionally complete. Not yet done: STDLIB.md doesn't need a new entry for this (serde substitution was already seeded there and covers it), but flagging for Role D to confirm.

**Blockers:** none. Working on branch `role-b/protocol-parsing`, not pushed to `main` directly — connection.rs (Role A) depends on these exact function signatures, didn't want to hand anyone a half-working intermediate state.

**Next up:** stretch items (wildcards, QoS 1) only after Role C's edge-case/interop test pass confirms core is solid, per PLAN.md §4 priority order.

### 2026-08-30 (later)

Implemented QoS 1 / PUBACK (PLAN.md §4 item 2):
- New `PubAckPacket` struct + `MqttPacket::PubAck` variant, `PT_PUBACK` constant.
- `decode_publish`/`encode_publish` extended to accept QoS 1 — packet identifier now required and parsed when `qos != 0`, validated non-zero per §2.3.1. QoS 2 still correctly rejected as `UnsupportedFeature` (only QoS 0/1 in scope).
- New `decode_puback`/`encode_puback`, with fixed-header-flags-must-be-zero validation (§3.4.1) and exactly-2-byte body validation (§3.4.2).
- `decode()`'s dispatch now accepts `PUBACK` as a legitimate packet *from* the client (unlike SUBACK/CONNACK/UNSUBACK/PINGRESP, which stay client→broker-illegal) — a client sends PUBACK to acknowledge a QoS 1 message the broker delivered to it.
- 12 new tests: QoS1 PUBLISH roundtrip, truncated/zero packet-id rejection, PUBACK roundtrip, wrong-body-length rejection, zero-packet-id rejection, nonzero-flags rejection. Updated two now-outdated tests (`publish_rejects_qos1_as_unsupported_in_core_scope` → replaced with actual QoS1 support tests; added a real QoS2-still-unsupported test in its place).

**Scope decision (see DECISIONS.md #9):** implemented the ack round-trip only, not full at-least-once redelivery (no DUP retransmission, no retry timers, no per-subscriber pending-ack tracking). PLAN.md §4 item 2 just says "QoS 1 (PUBACK)" — full redelivery semantics would be a much bigger broker-state undertaking than the remaining time budget supports honestly.

**Cross-file note:** adding the `PubAck` enum variant broke `connection.rs`'s exhaustive match (Role A's file) — had to add one minimal, clearly-commented arm there to keep `cargo build` passing. Flagged explicitly with a `// ROLE B ADDED THIS ARM` comment for their review; didn't restructure anything else in that file. This is a correct no-op (nothing to ack-track yet), not a stub.

**Status:** 62/62 tests passing, clean `cargo build` and `cargo build --release`.

**Not yet done:** broker→subscriber QoS1 delivery still doesn't track pending acks — that's real broker-state work belonging to `broker.rs` (Role A), left as an integration point, same pattern as wildcard fan-out.

### 2026-08-30 (bugfix, found by Role A's review)

Role A reviewed the QoS1 work and ran a live `mosquitto_pub -q 1` test against the actual built broker — it hung for the full timeout. Root cause: `connection.rs`'s `Publish` dispatch arm forwarded QoS1 messages to the broker channel but never sent the PUBACK back to the *publishing* client, which §3.3.4 requires. My earlier claim in this log and in DECISIONS.md #9 that "the ack round-trip is done and tested" was wrong for this direction — I'd only handled/tested the reverse case (client acking something the broker sent it, in the no-op `PubAck` arm) and every test I wrote was a `protocol.rs` encode/decode round-trip, which structurally cannot catch a missing connection-level behavior like this.

**Fix:** `connection.rs`'s `Publish` arm now pushes a `PUBACK` (echoing the packet identifier) onto the outbound queue immediately when `publish.qos == 1`, before forwarding to the broker channel. QoS 0 unaffected (no ack, as spec'd).

**Verified properly this time — live, not just unit tests:** installed `mosquitto-clients`, ran the exact repro Role A described (`mosquitto_pub -h 127.0.0.1 -p 1883 -q 1 -t test/qos1 -m hello -d`) against the compiled `release` binary. Confirmed it now completes immediately (`received PUBACK (Mid: 1, RC:0)` in the client's own debug log, exit code 0 — previously hung). Re-checked QoS 0 too, to make sure it still correctly gets no ack and doesn't hang either.

Full test suite re-run: still 62/62 passing (no regressions from the fix).

**Takeaway logged in DECISIONS.md #9:** encode/decode unit tests prove wire format, not broker behavior — anything claimed as an end-to-end "round-trip" needs a real client check (mosquitto/paho-mqtt), not just protocol-level tests, before being logged as done.