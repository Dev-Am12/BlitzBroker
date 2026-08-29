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
- [x] (stretch) Topic wildcards (+, #) — validation + matching predicate done; broker fan-out integration is a Role A step, see DECISIONS.md #9
- [ ] (stretch) QoS 1 (PUBACK)

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