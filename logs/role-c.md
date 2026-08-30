# Personal Log â€” Role C: Testing, Interop & Fuzzing

**Project:** BlitzBroker
**Owner:** Member C

## Scope (from PLAN.md Â§5)
Unit test suite for parsing edge cases, interop scripts against real mosquitto/paho-mqtt clients, multi-client concurrency/stress test â€” the project's headline verification work.

## Task queue
- [x] Unit tests: packet parsing edge cases (truncated, invalid remaining-length, oversized payload)
- [ ] Interop test: mosquitto_pub/mosquitto_sub against the broker
- [ ] Interop test: paho-mqtt (Python) scripted client against the broker
- [ ] Integration test: multi-client pub/sub fan-out correctness
- [x] Integration test: disconnect cleanup (no leaked subscriptions)
- [ ] Stress test: N concurrent clients, M topics â€” verify no data loss beyond documented drop-oldest backpressure behavior
- [ ] Write up verification results for README (the provable-correctness story)

## Log
_Add dated entries below as you go â€” what you did, decisions made, blockers hit._

## 2026-08-30 — Phase 1: unit tests for packet-parsing edge cases

Added two tests to the `#[cfg(test)]` module in `src/protocol.rs`:

- **`decode_max_remaining_length_never_panics`**: Feeds a 5-byte buffer whose
  remaining-length field encodes the MQTT 3.1.1 spec maximum (268,435,455 =
  `[0xFF, 0xFF, 0xFF, 0x7F]`, §2.2.3 Table 2.4) to `decode()`. Exercises the
  `header_len.checked_add(remaining_len as usize)` guard without triggering a
  panic or an index-out-of-bounds. Asserts `is_err()` rather than a specific
  variant because on a 64-bit host the overflow branch is not reachable (the
  buffer-too-short path fires first). See Personal_Decisions.md Decision 1 for
  the full judgment-call record.

- **`decode_large_publish_payload_roundtrip`**: Encodes a PUBLISH packet with a
  1 MiB payload (topic `load/test`, 1,048,576 bytes of 0xAB) and round-trips it
  through `encode()` / `decode()`. Verifies no panic, correct `consumed` byte
  count, and byte-exact payload preservation. Remaining length for this packet
  is 1,048,587 (4 RL bytes, §2.2.3), well within the spec's 268,435,455 limit.

**All 57 tests pass** (55 pre-existing + 2 new). No pre-existing tests modified.
No new dependencies added. No files outside `src/protocol.rs` (test module only),
`Personal_Decisions.md`, and `logs/role-c.md` were touched.


## 2026-08-30 — Phase 2: multi-topic disconnect cleanup test (broker.rs)

Added one test to the `#[cfg(test)]` module in `src/broker.rs`:

- **`disconnect_removes_subscriptions_across_multiple_topics`**: Registers a
  single client (id 10), subscribes it to three distinct topics (`alpha`,
  `beta`, `gamma`), sends `Disconnect { id: 10 }`, then publishes to all
  three topics and asserts the client's outbound queue remains empty. This
  closes the gap in the existing `disconnect_removes_all_subscriptions` test,
  which only exercises the single-topic case and therefore does not exercise
  the broker's `for subs in topics.values_mut()` loop across multiple HashMap
  entries.

Ordering note: `drop(tx)` + `broker.join()` are called *before*
`out_a.close()` + `pop_blocking()`. This ensures all three Publish messages
 are fully processed by the broker thread before the queue is inspected,
 eliminating any race between the broker writing to the queue and the test
 reading it. The existing single-topic test uses the reverse ordering
 (close then drop/join), which also works but leaves a smaller window for
 the broker to deliver before the close signals. Both orderings are correct
 for their respective tests.

**All 58 tests pass** (57 pre-existing + 1 new). No pre-existing tests modified.
No non-test code changed. No new dependencies added.

Checkbox status: `Integration test: disconnect cleanup` is now checked.
The combination of the existing single-topic test and this new multi-topic
test together fully covers the disconnect-cleanup behavior at the unit level.
Full integration testing against a real MQTT client remains in the queue.

