# BlitzBroker

**Zero Dependency Hackathon 2026 · Track C — Web & Network · Rust, `std` only**

> 🎬 **Demo video:** [Google Drive](https://drive.google.com/drive/folders/1ScvHPNuxs_JTPaasKMEvzKrL9Wo5OLah?usp=sharing)

BlitzBroker is a from-scratch MQTT 3.1.1 **subset** broker written in Rust. The shipped Rust program uses the standard library only: `Cargo.toml` has an empty `[dependencies]` section and `Cargo.lock` lists only the local `blitzbroker` package. It is intentionally a small, plaintext TCP broker — not a complete MQTT server and not a production security boundary.

This README is written for two audiences at once: judges scoring this submission against the event's rubric, and anyone who wants to build, run, or extend it. If you only read one other document, read [`DECISIONS.md`](./DECISIONS.md) — every non-mechanical engineering choice below, including the corrections we made after finding our own bugs, is expanded there with full reasoning.

---

## Table of contents

- [Quick start](#quick-start)
- [What this actually is](#what-this-actually-is)
- [Why an MQTT broker, why this shape of build](#why-an-mqtt-broker-why-this-shape-of-build)
- [Build and run](#build-and-run)
- [BlitzClient — included MQTT client](#blitzclient--included-mqtt-client)
- [Supported protocol surface](#supported-protocol-surface)
- [Topic names and wildcard filters](#topic-names-and-wildcard-filters)
- [Architecture and concurrency](#architecture-and-concurrency)
- [Connection lifecycle and queue policy](#connection-lifecycle-and-queue-policy)
- [Wire-format and parser behavior](#wire-format-and-parser-behavior)
- [Security and operational limits](#security-and-operational-limits)
- [Bonus criteria we are claiming](#bonus-criteria-we-are-claiming)
- [Scope completion ledger](#scope-completion-ledger)
- [Verification actually performed](#verification-actually-performed)
- [Dependency and build evidence](#dependency-and-build-evidence)
- [Reproducible build evidence](#reproducible-build-evidence)
- [Repository layout](#repository-layout)
- [Known gaps at submission time](#known-gaps-at-submission-time)
- [Single-file bonus — attempted, not shipped](#single-file-bonus--attempted-not-shipped)
- [License](#license)

---

## Quick start

**One-command build and run**, any OS with a Rust toolchain (edition 2021, rustc 1.97.1):

```bash
# Rust toolchain is pinned in rust-toolchain.toml (1.97.1). Cargo will auto-install it.
git clone https://github.com/Dev-Am12/BlitzBroker.git
cd BlitzBroker
cargo run --release -- --host 127.0.0.1 --port 1883
```

**Confirm the empty manifest and zero third-party dependency tree yourself:**

```bash
cargo tree --edges normal
# Expected: blitzbroker v0.1.0 (path to this checkout)
# — a single line, no third-party crates.
```

**Run the test suite:**

```bash
cargo test
# 118 passed (82 broker/protocol/queue + 36 blitzclient), 0 failed, at last verification (2026-08-31, see below).
```

**Submission evidence:** [`STDLIB.md`](./STDLIB.md) records the standard-library substitutions for every package we'd normally have installed; [`DECISIONS.md`](./DECISIONS.md) is the chronological engineering-decision log, including corrections. The [Known gaps](#known-gaps-at-submission-time) section below summarizes the submission status.

---

## What this actually is

A client connects over TCP, subscribes to exact topic names or MQTT-style topic filters, and publishes a byte payload. The broker routes the publish to every currently connected matching subscriber. The implementation includes:

- MQTT 3.1.1 wire framing and a hand-written packet codec.
- A TCP listener, one connection handler per accepted socket, and a dedicated writer thread per connection.
- Four independent broker-registry actor shards by default.
- Exact topic fan-out plus `+` and `#` wildcard-filter fan-out.
- QoS 0 and a deliberately limited QoS 1 publisher acknowledgement path.
- Retained messages: a PUBLISH with `retain=true` is stored per topic and immediately delivered to new subscribers on match; an empty retained payload clears the store (§3.3.1.3).
- Bounded outbound queues with a documented drop-oldest policy.
- Basic timestamped stdout/stderr logging and a small `--host`/`--port` CLI.

It is not a wrapper around an existing MQTT crate, and no third-party broker/client source was vendored into `src/`. Every packet-encoding, routing, and queueing decision below was written for this event.

---

## Why an MQTT broker, why this shape of build

Track C asks for something built on networking primitives and nothing above them, that speaks its protocol correctly enough to interoperate with real clients. MQTT was chosen specifically because a broker is not just a socket echo: it needs a real binary wire format with malformed-input rejection (Track B's concerns show up inside a Track C project), a routing layer that has to make an actual concurrency-model decision instead of hiding behind a framework, and a QoS story where "we did less than the spec" has to be stated honestly rather than glossed over.

We made the concurrency shape explicit rather than accidental: shard broker state across a fixed number of single-threaded actors addressed by `mpsc`, rather than either (a) a single global lock around one `HashMap`, which would have been simpler but a weaker demonstration of "handles concurrent connections without a framework," or (b) a lock-free concurrent map, which is closer to what a production broker library would pull in as a dependency. See `DECISIONS.md` #8 and #9 for the full reasoning and the tradeoffs accepted (see [Architecture and concurrency](#architecture-and-concurrency) below for what that costs).

---

## Build and run

Prerequisites:

- The toolchain is pinned via `rust-toolchain.toml` (`1.97.1`). If `rustup` is installed, it will automatically install the correct toolchain on first `cargo` invocation. Verification for this submission was performed with rustc 1.97.1 on `x86_64-pc-windows-msvc`. The code uses only `std::net`, `std::thread`, and `std::sync` — nothing platform-specific — so it is expected to build the same way on Linux and macOS, but that has not been independently verified on this worktree and is stated as an expectation, not a tested claim.
- A free TCP port. The default MQTT port is `1883`.

Build and start the broker in one command:

```bash
cargo run --release -- --host 127.0.0.1 --port 1883
```

Build without starting it:

```bash
cargo build --release
```

The release executable is `target/release/blitzbroker.exe` on Windows, or `target/release/blitzbroker` on Linux/macOS. If `--host` or `--port` is omitted, the broker defaults to `127.0.0.1:1883`.

CLI behavior is intentionally minimal:

- `--host <address>` changes the bind address (defaults to `127.0.0.1`).
- `--port <u16>` changes the port (defaults to `1883`).
- `--shards <usize>` changes the number of broker actor shards (defaults to `4`).
- An unrecognized argument is logged and ignored.
- A missing or non-numeric value after `--port` or `--shards` is ignored and leaves the default in place; it is not treated as a hard CLI error.
- A bind failure is logged and exits the process with status 1.

---

## BlitzClient — included MQTT client

`BlitzClient` (`src/bin/blitzclient.rs`) is a self-contained, zero-dependency MQTT 3.1.1 command-line client shipped alongside the broker. It exists to verify the broker end-to-end from a real MQTT client perspective without pulling in any external crate.

Build it alongside the broker:

```bash
cargo build --release
# produces target/release/blitzclient(.exe)
```

**Publish a message (QoS 0):**

```bash
cargo run --release --bin blitzclient -- --host 127.0.0.1 --port 1883 pub --topic sensors/temp --message 22.5C
```

**Publish with QoS 1 (expects a PUBACK):**

```bash
cargo run --release --bin blitzclient -- --host 127.0.0.1 --port 1883 pub --topic alerts/door --message open --qos 1
```

**Subscribe and print received messages (wildcard filter):**

```bash
cargo run --release --bin blitzclient -- --host 127.0.0.1 --port 1883 sub --topic "sensors/+/temp"
```

**CLI flags:**

| Flag | Required for | Description |
|---|---|---|
| `--host <address>` | both | Broker hostname or IP |
| `--port <u16>` | both | Broker port |
| `pub` | — | Subcommand: publish one message then disconnect |
| `sub` | — | Subcommand: subscribe and print messages until Ctrl-C |
| `--topic <name/filter>` | both | Topic name (pub) or filter (sub); accepts `+` and `#` wildcards for sub |
| `--message <text>` | pub | UTF-8 payload to publish |
| `--qos 0\|1` | pub | QoS level; defaults to 0; QoS 2 is rejected |

**Design notes:**
- BlitzClient does **not** import from `protocol.rs` — it re-implements MQTT wire encoding independently (see DECISIONS.md #10). This is intentional: it proves the broker's wire format is correct against an independent codec, not against its own encoder.
- The `sub` mode loops until the process is interrupted; each received PUBLISH is printed as `topic='…' payload='…' qos=N`.
- Its own test suite (`cargo test --bin blitzclient`) covers all encode/decode paths and accounts for 36 of the 118 total passing tests.

---

## Supported protocol surface

The parser accepts MQTT protocol name `MQTT` and protocol level `4` (MQTT 3.1.1). The table describes the **current code path**, not a claim of full MQTT 3.1.1 conformance.

| Packet / feature | Current behavior | Important boundary |
|---|---|---|
| `CONNECT` → `CONNACK` | Parses client ID, clean-session bit, and keep-alive value. The broker registers the connection and replies `CONNACK Accepted` with `session_present = false`. | No authentication or session restoration. Will, username, and password flags cause rejection. The parsed clean-session and keep-alive values are not used to implement session or timeout behavior. |
| `SUBSCRIBE` → `SUBACK` | Accepts one or more validated topic filters and sends one return code per filter, in the same order. The return code is always `0x00` (granted QoS 0). | Requested subscription QoS values 0, 1, and 2 are parsed, but delivery is granted/reported at QoS 0. No per-subscription QoS negotiation or persistence exists. |
| `UNSUBSCRIBE` → `UNSUBACK` | Removes that connection ID from the named filters and echoes the request packet ID. | No persistent subscription state exists after disconnection. |
| `PUBLISH` QoS 0 | Routes the message to current exact and wildcard subscribers. | Fire-and-forget only; no packet ID and no acknowledgement. |
| `PUBLISH` QoS 1 → `PUBACK` | Requires a non-zero packet ID. The broker queues a matching `PUBACK` to the publishing connection before forwarding the publish. | This is not complete at-least-once delivery: subscriber acknowledgements are not tracked, no retry timer exists, and no redelivery/DUP behavior is implemented. |
| `PUBACK` from a client | Decoded as a valid MQTT packet and treated as a no-op. | There is no pending-delivery state to clear because subscriber QoS 1 tracking is not implemented. |
| `PINGREQ` → `PINGRESP` | Responds locally from the connection handler. | Keep-alive values are not enforced; a silent client is not proactively disconnected. |
| `DISCONNECT` | Ends the connection path and broadcasts cleanup to all shards. | Abrupt socket close/error also triggers cleanup, but there is no last-will publish. |

MQTT QoS 2 is rejected. A PUBLISH whose QoS flag bits are `11` is rejected as malformed.

---

## Topic names and wildcard filters

Published topic names must be non-empty and cannot contain `+` or `#`. Subscription and unsubscription filters are validated before being sent to the broker:

- `+` must occupy an entire topic level, such as `sensors/+/temp`.
- `#` must occupy an entire final topic level, such as `sensors/#` or simply `#`.
- Empty filters, `sport#`, `sport/#/rank`, and `sport+` are rejected.
- Matching is iterative rather than recursive, including for adversarial slash-heavy strings.
- A matching `+` consumes exactly one level; a final `#` consumes zero or more remaining levels.

Exact subscriptions are hash-routed to one shard. Wildcard subscriptions are broadcast to every shard because a concrete topic and the matching wildcard string generally hash differently. During delivery, the publish shard uses an exact-match fast path, then checks wildcard filters, and tracks already-notified connection IDs so a client subscribed both exactly and through a matching wildcard does not receive the same publish twice.

---

## Architecture and concurrency

```text
TCP listener
  └─ accepted socket → connection handler
       ├─ reader: socket bytes → decode → BrokerMessage
       ├─ writer thread: outbound queue → encode → socket bytes
       └─ ShardedBroker router
            ├─ shard 0: client map + topic/filter map
            ├─ shard 1: client map + topic/filter map
            ├─ shard 2: client map + topic/filter map
            └─ shard 3: client map + topic/filter map
```

Each shard processes its own `mpsc` receiver serially and exclusively owns its registry maps. Concrete `Subscribe`, `Unsubscribe`, and `Publish` messages go to the shard selected by `DefaultHasher(topic) % shard_count`. `Register` and `Disconnect` are broadcast because a connection may subscribe on any shard and disconnect cleanup must reach every shard. Wildcard filter subscribe/unsubscribe messages are also broadcast.

Consequences and limits:

- Registry mutation does not use a shared concurrent map; each map is actor-owned.
- A concrete topic always routes to one shard, so that shard serially processes messages it receives for the topic. There is no global ordering guarantee across different shards/topics.
- Every shard keeps a client entry for every registered connection, even if the connection has no subscription on that shard. This is intentional replication for routing and cleanup, and it is a real scalability tradeoff we are not hiding: it costs O(shards) state per connection instead of O(1).
- The default number of shards is `NUM_BROKER_SHARDS = 4`, configurable at runtime via `--shards <N>`.
- This project has not established throughput, latency, capacity, or fairness benchmarks. Do not infer a performance claim from the sharded design.

---

## Connection lifecycle and queue policy

For each accepted TCP socket, the handler clones the stream, starts a writer thread, and reads/decodes on the handler thread. Decoded packets are drained from the read buffer until the next packet is incomplete. A malformed packet or socket read error logs a warning, sends a broker `Disconnect`, closes the outbound queue, and ends the handler.

Each connection has an outbound queue of 128 events. The queue is built from `VecDeque`, `Mutex`, `Condvar`, and `Arc`:

- Under capacity it behaves FIFO.
- At capacity, a new event removes the oldest queued event and then appends the new event.
- Producers do not block waiting for a slow subscriber; the trade-off is intentional message loss under overload.
- Closing the queue wakes a blocked writer. A writer exits after the queue is closed and drained, or after a socket write error.

This queue policy applies to broker-to-client outbound packets only. There is no durable queue, disk persistence, reconnect replay, delivery receipt accounting, or broker-wide backpressure mechanism.

---

## Wire-format and parser behavior

The codec implements MQTT fixed headers, MQTT variable-length Remaining Length fields, two-byte length-prefixed UTF-8 fields, and packet-specific bodies. It handles one complete packet at a time and reports `Incomplete` when more socket bytes are needed.

Parser checks that are present:

- Remaining Length uses at most four bytes; a fifth continuation byte is rejected.
- The decoded total packet length uses checked addition.
- Empty/truncated buffers remain incomplete rather than being indexed unsafely.
- UTF-8 length prefixes and UTF-8 validity are checked before constructing strings.
- `PINGREQ` and `DISCONNECT` require zero Remaining Length.
- `PUBACK` requires zero fixed-header flags, exactly a two-byte, non-zero packet ID.
- QoS 1 PUBLISH requires a non-zero packet ID.
- Broker-to-client packet types received from a client (`CONNACK`, `SUBACK`, `UNSUBACK`, `PINGRESP`) are rejected.

Important parser/encoder boundaries:

- There is **no configured application-level maximum inbound packet or payload size**. The MQTT Remaining Length format caps the encoded value at 268,435,455, but the connection reader accumulates bytes until a full packet arrives. The maximum-header test checks error/no-panic behavior; it is not a memory-exhaustion guarantee.
- Fixed-header reserved-flag validation is not exhaustive for every packet type. The explicit checks listed above exist; callers should not treat the parser as a full conformance validator.
- CONNECT handling rejects will/username/password flags, but does not implement every MQTT CONNECT-flag consistency rule.
- The PUBLISH DUP flag is not represented in `PublishPacket`: incoming DUP is not preserved, and outbound encoded PUBLISH packets always set DUP to 0.
- The RETAIN bit is parsed, stored per-topic, and replayed to new subscribers. An empty-payload retained PUBLISH clears the stored message (§3.3.1.3). The DUP flag is not preserved on forwarded publishes.
- Outbound UTF-8 encoding truncates a string longer than 65,535 bytes to the representable prefix instead of returning an encode error. That avoids an indexing panic but is not a substitute for full oversize-output handling.
- The connection handler does not enforce an MQTT state machine such as CONNECT-first. It dispatches supported packets as they arrive; well-behaved clients should still CONNECT before subscribing or publishing.

---

## Security and operational limits

- Transport is plaintext TCP only. TLS is intentionally excluded: Rust `std` does not provide the cryptographic/TLS primitives needed to implement TLS safely from scratch.
- No username/password authentication, ACLs, authorization, or tenant isolation exists.
- No persistent sessions, session takeover handling for duplicate MQTT client IDs, or offline-message storage exists. Internally, connections are keyed by a monotonically allocated local `AtomicU64` ID rather than the MQTT client ID.
- No last-will messages or keep-alive timeout enforcement exists.
- A client that sends an unsupported feature or malformed input is disconnected; there is no MQTT error-response framework beyond the packet replies listed above.
- The broker is not an MQTT client and does not connect to upstream/external brokers.
- This is not a hardened internet-facing service. Put it behind appropriate network controls if used outside local development.

---

## Bonus criteria we are claiming

Honest status against each of the four optional bonus categories, cross-checked against the current worktree at time of writing (2026-08-31), not against what was originally planned. We are not claiming anything here we can't point a judge to directly.

| Bonus | Status | Evidence |
|---|---|---|
| **Package Killer (+3)** | **Claiming.** | The packages a developer would normally `npm install` for this exact local pub/sub use case are `mqtt` and `aedes` (both have substantial weekly download counts). This project reimplements the equivalent surface from scratch: MQTT 3.1.1 packet encode/decode (`protocol.rs`), QoS 0/1 publish and PUBACK handling, and topic-filter subscribe/publish routing (`broker.rs`), all on `std` alone. Rationale recorded in `DECISIONS.md` #4; the substitution itself is documented per-feature in `STDLIB.md`. |
| **STDLIB Log (+3)** | **Claiming.** | `STDLIB.md` documents **13** implemented substitutions across: async runtime, concurrent registry, bounded queue, CLI parsing, logging, packet codec, QoS/PUBACK, topic-filter matching, connection IDs, adversarial byte fuzz-testing, standalone interop scripts, retained-message store, and benchmark timing — all non-trivial, all traceable to a file. The full list is in [`STDLIB.md`](./STDLIB.md). |
| **Single File (+5)** | **Not achieved.** | A partial flatten exists on the `single-file` branch (`submission/blitzbroker.rs`, ~3,885 lines) but it preserves `mod` boundaries wholesale — a stapled concatenation, not a true single-module program. Not claiming this bonus; the branch is public for judges to inspect. |
| **Reproducible Build (+5)** | **Achieved & Claiming.** | Two consecutive clean builds and two builds from different directory paths under rustc 1.97.1 all produced the exact same SHA-256 hash. Toolchain is pinned via `rust-toolchain.toml`; MSVC PE timestamp non-determinism is fixed via `-C link-arg=/Brepro`; absolute paths are remapped via `--remap-path-prefix`; `codegen-units=1` and `debug=false` are configured in `[profile.release]`. Full proof in [`proof/reproducible-build.md`](proof/reproducible-build.md) and `DECISIONS.md` #14. |

**Net bonus claim: +11 (Package Killer +3, STDLIB Log +3, Reproducible Build +5).** Single File is disclosed as missed rather than asserted and left for a judge to disprove.

---

## Scope completion ledger

| PLAN.md extra item | Status | Detail |
|---|---|---|
| Topic wildcards | Completed | Validation, matching, broker fan-out, de-duplication, and cross-shard broadcast routing are implemented. |
| QoS 1 / `PUBACK` | completed | Publisher receives a matching PUBACK. |
| Sharded actor registry | Completed | Four hash-routed single-threaded actor shards are used by production startup. |
| Retained messages | Completed | A PUBLISH with `retain=true` stores the message per topic (or clears it on empty payload, per §3.3.1.3). New subscribers receive the most recent retained message immediately on match, including wildcard subscribers via shard broadcast. |
| Last-will messages | Skipped | CONNECT with a will flag is rejected. |
| Keep-alive enforcement | Skipped | Keep-alive is parsed but not used for timeout/disconnect. |
| Reproducible build | **Achieved** | Two consecutive clean builds and two builds from different directory paths under rustc 1.97.1 all produced the same SHA-256. Toolchain pinned via `rust-toolchain.toml`; MSVC PE timestamp fixed via `-C link-arg=/Brepro`; paths remapped via `--remap-path-prefix`; `codegen-units=1`, `debug=false` in `[profile.release]`. Proof in `proof/reproducible-build.md`. |
| Single-file merge | Attempted, not achieved | A ~3,885-line flatten on `origin/single-file` preserves `mod` boundaries intact — not a true single-module program. Not claiming the bonus; branch is public. |

Out of scope: QoS 2, MQTT 5.0, persistent sessions, authentication/ACLs, encrypted transport, and a complete MQTT compliance implementation.

---

## Verification actually performed

The following were personally run against this worktree on 2026-08-31:

- `cargo test` completed with **118 passed, 0 failed** (82 in the main library harness covering broker, protocol, connection, and queue; 36 in the `blitzclient` binary harness).
- The release build completed successfully (with non-fatal dead-code warnings in the current source).
- `python tests/interop/paho_client.py` passed: a paho-mqtt publisher and subscriber exchanged a byte-exact QoS 0 payload through the release broker.
- A separate live paho MQTT 3.1.1 check passed: a QoS 1 publisher received its PUBACK, and a subscriber on `role-d/+/temp` received the publish to `role-d/kitchen/temp` at QoS 1.
- `cargo tree --edges normal` listed only `blitzbroker v0.1.0`; output saved in [`proof/cargo-tree.txt`](proof/cargo-tree.txt) and [`deps-proof.txt`](./deps-proof.txt).
- Reproducible build verification: byte-identical SHA-256 binary hash across consecutive clean builds and across different build directories (see [`proof/reproducible-build.md`](proof/reproducible-build.md)).

The automated test suite includes protocol, broker, connection, and queue coverage. Its named coverage includes malformed/truncated/unknown packet handling; Remaining Length boundaries; UTF-8 validation; PUBLISH QoS 0/1 and PUBACK validation; topic-filter validation/matching; disconnect cleanup; bounded queue behavior; drop-oldest stress accounting; exact fan-out; shard isolation; wildcard delivery/unsubscribe; QoS1 PUBACK packet-ID matching; and retained-message store/clear/replay.

---

## Dependency and build evidence

The dependency-proof command is `cargo tree --edges normal`, run directly:

```bash
cargo tree --edges normal
```

Expected output (path differs per checkout, everything else is identical):

```text
blitzbroker v0.1.0 (/path/to/checkout)
```

A saved copy of this output is committed at [`proof/cargo-tree.txt`](proof/cargo-tree.txt) and [`deps-proof.txt`](./deps-proof.txt).

`scripts/dependency-proof.cmd` is a one-line Windows wrapper around that same command kept for convenience; judges on any OS should run `cargo tree --edges normal` directly.

`STDLIB.md` documents the 13 standard-library replacements for async/networking, registries, queueing, CLI parsing, logging, packet encoding, QoS1/PUBACK, wildcard matching, retained-message store, adversarial byte fuzz-testing, standalone interop scripts, and connection IDs.

## Reproducible build evidence

Reproducible builds (+5 bonus) are achieved via four mechanisms:

1. **`rust-toolchain.toml`** — pins `rustc 1.97.1`; `rustup` installs it automatically.
2. **`.cargo/config.toml`** — two `rustflags` entries:
   - `--remap-path-prefix =blitzbroker/` — rewrites all absolute checkout paths embedded in the binary to the stable relative prefix `blitzbroker/`.
   - `-C link-arg=/Brepro` — Windows/MSVC-specific: instructs the linker to replace the `TimeDateStamp` field in the PE COFF header (offset 0xF0) with a content-derived hash instead of the current wall-clock time. Without this flag every Windows build is unique at the binary level regardless of source inputs.
3. **`[profile.release]`** in `Cargo.toml` — `codegen-units=1` (single deterministic CGU) and `debug=false` (removes debug-info section entirely).

Two consecutive **clean** builds and two builds from **different directory paths** all produced the same SHA-256 hash. Full proof (hashes, method, reproduce-yourself command) is in [`proof/reproducible-build.md`](proof/reproducible-build.md).

---

## Repository layout

```text
BlitzBroker/
├── .cargo/
│   └── config.toml             # compiler flags: path remapping & /Brepro linker flag
├── proof/
│   ├── cargo-tree.txt          # captured `cargo tree --edges normal` output
│   └── reproducible-build.md   # byte-exact SHA-256 verification evidence
├── src/
│   ├── main.rs                 # entry point: CLI parsing, TCP listener, ShardedBroker startup
│   ├── broker.rs               # ShardedBroker actor: registry, publish fan-out, retained messages
│   ├── connection.rs           # per-connection handler: decode loop, writer thread, QoS1 PUBACK
│   ├── protocol.rs             # MQTT 3.1.1 packet codec: encode + decode, wildcard validation
│   ├── queue.rs                # bounded drop-oldest outbound queue (VecDeque + Mutex + Condvar)
│   ├── error.rs                # ProtocolError / BrokerError types
│   ├── logging.rs              # minimal levelled logger (std::time, stdout/stderr)
│   └── bin/
│       ├── blitzclient.rs      # standalone MQTT pub/sub CLI client (independent codec, 36 tests)
│       └── shard_benchmark.rs  # throughput benchmark for the sharded broker (std::time::Instant)
├── scripts/
│   └── dependency-proof.cmd    # Windows convenience wrapper for `cargo tree --edges normal`
├── tests/
│   └── interop/
│       ├── paho_client.py      # Python end-to-end test: QoS 0 pub/sub via paho-mqtt
│       ├── mosquitto.sh        # Bash interop script using mosquitto_pub / mosquitto_sub
│       └── mosquitto-docker/
│           └── mosquitto.conf  # Mosquitto config for the Docker-based interop environment
├── Cargo.toml                  # [dependencies] is empty; release profile for reproducibility
├── Cargo.lock                  # locked to blitzbroker only
├── rust-toolchain.toml         # pinned compiler toolchain (1.97.1)
├── LICENSE                     # MIT
├── README.md                   # this file
├── STDLIB.md                   # std substitution log (13 entries)
├── DECISIONS.md                # chronological engineering-decision record (Decisions 1-14)
├── SUBMISSION_CHECKLIST.md     # pre-submission verification checklist
├── deps-proof.txt              # duplicate captured dependency tree output
├── .zero-dep.toml              # track letter + one-line pitch
└── .gitignore
```

`target/` is generated by Cargo and is not committed. The `single-file` branch contains a partial flatten attempt (`submission/blitzbroker.rs`) — see [Single-file bonus](#single-file-bonus--attempted-not-shipped) below.

---

## Known gaps at submission time

This section exists so a judge does not have to find these out the hard way. As of the last audit (2026-08-31):

- **Demo video:** available at [Google Drive](https://drive.google.com/drive/folders/1ScvHPNuxs_JTPaasKMEvzKrL9Wo5OLah?usp=sharing).
- **The Single File bonus (+5) was attempted but not achieved.** A ~3,885-line flatten exists on `origin/single-file` but preserves `mod` boundaries intact — not a true single-module program. Not claiming the bonus.
- **`Personal_Decisions.md` is cited in early logs but missing.** Older internal logs and source comments reference a `Personal_Decisions.md` for several engineering rationales. It is not present in this worktree or in any recoverable branch/reflog/stash history. That rationale has not been reconstructed or invented to fill the gap — `DECISIONS.md` #10 is the authoritative record of this specific documentation loss.

See [`STDLIB.md`](./STDLIB.md) for the zero-dependency substitutions and [`DECISIONS.md`](./DECISIONS.md) for the full engineering-decision record.

---

## Single-file bonus — attempted, not shipped

The `single-file` branch contains the work done towards the "single Rust file" bonus. The branch reached a compilable state: `submission/blitzbroker.rs` is a ~3,885-line file that builds and passes the same test suite. However, the flatten was not a true merge: `mod error;`, `mod logging;`, `mod protocol;`, `mod queue;`, `mod connection;`, and `mod broker;` declarations and their inner module boundaries were preserved wholesale from the multi-file layout, making the result a stapled concatenation rather than a genuine single-module program. Resolving all name-collision and visibility issues to produce a real single-module flatten (all items at the crate root, no inner `mod` wrappers other than `#[cfg(test)]` blocks) would have required more time than was available at code freeze.

**Decision:** the `single-file` branch is preserved for reference and to record the attempt honestly. The submission target is `main`, which uses the multi-file layout with zero third-party dependencies. No single-file bonus is claimed.

The branch remains publicly visible at `origin/single-file` for any judge who wishes to inspect the partial work.

---

## License

MIT. See [`LICENSE`](./LICENSE).