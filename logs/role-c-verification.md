# BlitzBroker — Verification Results (Role C hand-off to Role D)

**Prepared by:** Member C (Testing, Interop & Fuzzing)
**Date:** 2026-08-30
**Intended audience:** Role D, for incorporation into README.md

> **Note for Role D:** This document contains only claims that were
> actually run and observed during Phases 1–4. Limitations are disclosed
> explicitly rather than omitted. Copy and edit as appropriate for the
> README narrative — do not promote any item from "not run" to "verified"
> without running it first.

---

## 1. Automated unit test suite

**Tool:** `cargo test` (standard Rust test runner, no external tools required)
**Final count as of 2026-08-30:** **66 tests, 0 failed**

Run command:
```
cargo test
```

All 66 tests pass. The suite covers three modules:

### 1.1 Protocol parsing (`src/protocol.rs`) — 51 tests

Tests verify the packet codec against MQTT 3.1.1, covering:

| Area | Selected test names |
|---|---|
| CONNECT | `connect_roundtrip`, `connect_rejects_wrong_protocol_name`, `connect_rejects_unsupported_protocol_level`, `connect_truncated_mid_variable_header_is_malformed_not_panic` |
| PUBLISH QoS 0 | `publish_qos0_roundtrip`, `publish_empty_payload_is_valid`, `publish_rejects_empty_topic_name`, `publish_rejects_wildcard_in_topic_name`, `publish_rejects_qos2_reserved_value` |
| PUBLISH QoS 1 | `publish_qos1_roundtrip`, `publish_qos1_rejects_zero_packet_identifier`, `publish_qos1_rejects_truncated_packet_identifier` |
| PUBACK | `puback_roundtrip`, `puback_rejects_zero_packet_identifier`, `puback_rejects_nonzero_flags`, `puback_rejects_wrong_body_length` |
| SUBSCRIBE / SUBACK | `subscribe_roundtrip_multiple_topics`, `subscribe_rejects_empty_topic_list`, `subscribe_rejects_invalid_qos`, `subscribe_accepts_valid_wildcard_filter`, `subscribe_rejects_invalid_wildcard_filter` |
| UNSUBSCRIBE | `unsubscribe_roundtrip` |
| PINGREQ/PINGRESP | `decode_pingreq_roundtrip`, `decode_pingreq_rejects_nonzero_remaining_length` |
| Remaining-length encoding | `remaining_length_roundtrip_single_byte`, `remaining_length_roundtrip_two_bytes`, `remaining_length_max_value_four_bytes`, `remaining_length_rejects_five_continuation_bytes`, `remaining_length_incomplete_never_panics` |
| Wildcard filter validation | `validate_filter_accepts_plus_at_any_level`, `validate_filter_accepts_hash_alone`, `validate_filter_accepts_hash_as_last_level`, `validate_filter_rejects_hash_not_last`, `validate_filter_rejects_plus_not_alone_in_level`, `validate_filter_rejects_hash_not_alone_in_level`, `validate_filter_rejects_empty_filter` |
| Topic matching | `matches_exact_topic`, `matches_single_level_wildcard`, `matches_multi_level_wildcard`, `matches_plus_does_not_match_missing_level`, `matches_leading_slash_topics`, `matches_never_panics_on_adversarial_slash_counts` |
| UTF-8 decoding | `utf8_string_rejects_invalid_utf8_bytes`, `utf8_string_rejects_length_exceeding_buffer`, `utf8_string_rejects_truncated_length_prefix` |
| Edge cases (Phase 1 additions) | `decode_max_remaining_length_never_panics`, `decode_large_publish_payload_roundtrip` |
| Panic-safety / fuzz-guard | `decode_empty_buffer_is_incomplete_not_panic`, `decode_truncated_after_fixed_header_is_incomplete`, `decode_unknown_packet_type_is_rejected`, `decode_only_consumes_one_packet_leaves_rest_buffered`, `decode_never_panics_on_random_bytes` |

**Phase 1 additions specifically:**

- `decode_max_remaining_length_never_panics`: feeds the MQTT 3.1.1 spec-maximum
  remaining length (268,435,455, §2.2.3) in a 5-byte buffer to `decode()`.
  Asserts `is_err()` — no panic. On 64-bit hosts the `Incomplete` path fires
  (buffer too short); the `checked_add` overflow path is unreachable on 64-bit
  by arithmetic (5 + 268,435,455 = 268,435,460, far below `usize::MAX`). The
  test is honest about this: it uses `is_err()` not a specific variant assertion.
  See Personal_Decisions.md Decision 1 for the full rationale.

- `decode_large_publish_payload_roundtrip`: encodes a PUBLISH with a 1 MiB
  payload (topic `load/test`, 1,048,576 bytes of 0xAB) and round-trips it
  through `encode()` / `decode()`. Asserts no panic, correct `consumed` byte
  count, and byte-exact payload preservation.

### 1.2 Broker logic (`src/broker.rs`) — 4 tests

| Test | What it verifies |
|---|---|
| `publish_fans_out_to_all_subscribers` | A PUBLISH to a topic reaches every subscriber of that topic |
| `disconnect_removes_all_subscriptions` | A Disconnect removes the client from one topic's subscriber list |
| `disconnect_removes_subscriptions_across_multiple_topics` | A Disconnect removes the client from ALL topics (3-topic case, added in Phase 2) |
| `stress_no_data_loss_beyond_drop_oldest` | No messages silently lost under load; drop-oldest policy correct (added in Phase 3, see §2) |

### 1.3 Queue (`src/queue.rs`) — 3 tests

`drop_oldest_when_full`, `close_wakes_blocked_consumer`, `fifo_order_preserved_under_capacity`

### 1.4 Connection layer (`src/connection.rs`) — 8 tests

SUBSCRIBE→SUBACK and UNSUBSCRIBE→UNSUBACK correctness, packet_id echo, multi-topic handling.

---

## 2. In-process concurrency and backpressure stress test

**Test name:** `broker::tests::stress_no_data_loss_beyond_drop_oldest`
**Location:** `src/broker.rs` `#[cfg(test)]` module
**Run:** `cargo test stress_no_data_loss_beyond_drop_oldest`
**Result:** PASS (verified twice consecutively for flakiness; both runs identical)

The test has two scenarios driven directly against `run_broker` and the real
`queue` module — no TCP sockets, no timing-dependent sleeps:

**Scenario A — normal load (no drops):**
- N = 20 clients, M = 5 topics
- Each client subscribes to exactly one topic (round-robin: client i → topic i % 5)
- 50 messages published to each topic (250 total)
- 50 < `DEFAULT_CLIENT_QUEUE_CAPACITY` (128) → zero drops expected
- Assertion: every client's queue drains to exactly 50 items
- All 20 clients passed; any deviation would mean silent loss or mis-routing

**Scenario B — over-capacity load (drop-oldest path):**
- 1 client, 1 topic, 148 messages published (= CAPACITY + 20)
- Queue retains at most 128 items (drops oldest when full)
- Assertion: queue drains to exactly 128 items
- Verifies: (a) drop-oldest policy fires correctly, (b) no further loss beyond it

The test is structurally race-free: the broker is a serial actor; all
`BrokerMessage`s are enqueued before the channel is dropped; `broker.join()`
guarantees completion before any queue is inspected.

---

## 3. Interoperability verification

### 3.1 paho-mqtt ↔ BlitzBroker — VERIFIED, PASS

**Command run:**
```
python tests\interop\paho_client.py
```

**Observed output (exit 0):**
```
INFO: Starting blitzbroker on 127.0.0.1:18831 ...
INFO: Waiting for broker to become ready ...
INFO: Broker is ready.
PASS: paho-mqtt round-trip OK.
      Published: "hello-from-paho-5616"
      Received:  "hello-from-paho-5616"
```

**Rust artifact:** `target/release/blitzbroker.exe` (started as subprocess by the script)
**paho-mqtt version:** v2 (installed; script required a 2-line fix for the
`CallbackAPIVersion.VERSION1` API change — see Personal_Decisions.md Decision 5)
**MQTT endpoint:** `127.0.0.1:18831`
**Topic:** `blitz/interop/paho-smoke`
**Protocol:** MQTT 3.1.1

**What this test exercises end-to-end:**
- paho subscriber connects → BlitzBroker sends CONNACK ✓
- paho subscriber sends SUBSCRIBE → BlitzBroker sends SUBACK ✓
- paho publisher connects → BlitzBroker sends CONNACK ✓
- paho publisher sends PUBLISH → BlitzBroker routes to subscriber ✓
- paho subscriber receives PUBLISH with byte-exact payload ✓

**Direction:** paho_pub → Rust broker → paho_sub (both directions through broker verified)

**Not tested:** Rust acting as an MQTT client to an external broker — BlitzBroker
is a broker, not a client, so this direction does not exist in the current architecture.

---

### 3.2 mosquitto ↔ BlitzBroker (via Docker) — VERIFIED, PASS

**Docker image:** `eclipse-mosquitto:latest`
(sha256:`6f8d8a947c506f8a2290ec65cd4bd2bc7cb4d43fb5f6271f861cb013e2ef9797`)

**Setup:** Minimal config (`listener 1883 / allow_anonymous true`) in
`tests/interop/mosquitto-docker/mosquitto.conf`.

**Step B — Mosquitto broker self-sanity (no Rust involved):**
```sh
docker exec blitz-mosquitto sh -c "
  mosquitto_sub -h 127.0.0.1 -p 1883 -t 'test/sanity' -C 1 -W 5 &
  sleep 0.3 && mosquitto_pub -h 127.0.0.1 -p 1883 -t 'test/sanity' -m 'broker-sanity-ok'
  wait && cat /tmp/sub_out.txt"
```
Received: `"broker-sanity-ok"` — Mosquitto broker verified independently. ✓
(This step does NOT count as Rust interoperability.)

**Step C — mosquitto ↔ Rust:**
```
BlitzBroker: target/release/blitzbroker.exe --host 0.0.0.0 --port 18832
mosquitto_sub (Docker) → host.docker.internal:18832 → receives payload
mosquitto_pub (Docker) → host.docker.internal:18832 → routed by Rust broker
```

**Observed:**
```
mosquitto_sub stdout: 'hello-rust-from-docker-37013'
```
Published payload: `hello-rust-from-docker-37013` — exact match ✓

**Direction verified:** `mosquitto_pub → BlitzBroker → mosquitto_sub`

**Direction NOT verified:** `BlitzBroker → mosquitto` acting as an MQTT client.
BlitzBroker is a broker, not an MQTT client; it does not connect to external
brokers. There is no "Rust publishes to Mosquitto" path in the architecture.

**Networking note (Windows-specific):** On Docker Desktop for Windows,
`host.docker.internal` is the hostname by which containers reach the Windows
host (resolves to `192.168.65.254` via Docker's internal DNS). The Rust broker
must remain alive as a subprocess during the test — it cannot be started as a
detached background process via PowerShell between commands due to process
lifetime scoping.

---

## 4. Interop scripts (manual developer tools)

Two scripts exist in `tests/interop/`:

| Script | Tool required | Skip behaviour |
|---|---|---|
| `mosquitto.sh` | `mosquitto_pub`, `mosquitto_sub` in PATH | Exits 0 with SKIP notice if absent |
| `paho_client.py` | `paho-mqtt` Python package | Exits 0 with SKIP notice if `import paho` fails |

Both scripts start the broker as a subprocess, wait for TCP readiness, run
the round-trip, verify the payload, and kill the broker on exit (including
on failure). They are **not wired into `cargo test`** — they are developer-run
manual tools. See Personal_Decisions.md Decision 4 for the rationale.

`mosquitto.sh` was **not** run on the development machine (Windows, no WSL,
no native `mosquitto` CLI). Its skip path was not directly executed. The
script was reviewed for correctness and will produce a SKIP exit-0 on any
machine without `mosquitto_pub` in PATH.

`paho_client.py` was run and produces the PASS result described in §3.1 above.

---

## 5. Disclosed limitations and known gaps

The following items are explicitly not claimed as verified:

| Item | Status | Notes |
|---|---|---|
| Multi-client pub/sub fan-out integration test (live TCP) | Not run | Covered at the unit level by `publish_fans_out_to_all_subscribers`; full TCP integration test was descoped to make room for real interop runs |
| Wildcard-subscription fan-out in live broker | Not unit-tested at broker level | `validate_filter_*` and `matches_*` tests verify the filter logic in `protocol.rs`; the broker's `Subscribe` handler uses exact-match topic strings — wildcard routing at fan-out time is out of core scope per PLAN.md §4 |
| `mosquitto.sh` actually executed | Not run | Tool absent from dev machine; script reviewed manually |
| QoS 1 end-to-end through live broker | Not tested by interop scripts | Protocol codec tests cover QoS 1 packet encode/decode (`publish_qos1_roundtrip`, `puback_roundtrip`); the broker routes QoS 1 at QoS 0 semantics (no PUBACK from broker, no retry) per PLAN.md §4 |
| Stress test under real TCP connections | Not run | Stress test operates in-process via `mpsc` channels; a live TCP stress test (N simultaneous TCP connections) was not implemented |

---

## 6. Summary for README.md

The following claims are verified and may be included in README:

- `cargo test` passes with 66 tests, 0 failures, using `std` only (no test-only external crates).
- Packet parsing returns errors on malformed/truncated/oversized input — no panics observed on adversarial input, including random-byte fuzzing (`decode_never_panics_on_random_bytes`) and the MQTT-spec-maximum remaining-length encoding.
- A 1 MiB PUBLISH payload round-trips through encode/decode with byte-exact preservation.
- Fan-out, subscription management, and disconnect cleanup are covered by unit tests against the real broker actor.
- Under load (20 clients, 5 topics, 50 msg/topic in-process): zero messages silently lost. When the outbound queue is deliberately overflowed (148 messages, capacity 128), exactly 128 survive — consistent with the documented drop-oldest policy.
- paho-mqtt (MQTT 3.1.1, Python, v2 API) interoperates with BlitzBroker: publish and subscribe round-trip verified with byte-exact payload delivery. Tested on Windows, port 18831, topic `blitz/interop/paho-smoke`.
- `mosquitto_pub` / `mosquitto_sub` (via Docker `eclipse-mosquitto:latest`) interoperate with BlitzBroker: publish routed to subscriber with byte-exact delivery. Tested on Windows with Docker Desktop, port 18832, topic `blitz/interop/mosquitto-rust`.
- BlitzBroker is a broker, not an MQTT client — it does not connect to external brokers. "Rust → external broker" is not a supported or tested direction.
