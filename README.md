# BlitzBroker

BlitzBroker is a from-scratch MQTT 3.1.1 **subset** broker written in Rust. The shipped Rust program uses the standard library only: `Cargo.toml` has an empty `[dependencies]` section and `Cargo.lock` lists only the local `blitzbroker` package. It is intentionally a small, plaintext TCP broker—not a complete MQTT server and not a production security boundary.

**Track C — Web & Network.**

## Quick start

```bash
# Rust toolchain is pinned in rust-toolchain.toml (1.97.1). Cargo will auto-install it.
git clone https://github.com/Dev-Am12/BlitzBroker.git
cd BlitzBroker
cargo run --release -- --host 127.0.0.1 --port 1883
```

Confirm the empty manifest and zero third-party dependency tree yourself:

```bash
cargo tree --edges normal
# Expected: blitzbroker v0.1.0 (path to this checkout)
# — a single line, no third-party crates.
```

Run the test suite:

```bash
cargo test
# 77 passed, 0 failed, at last verification (2026-08-31, see below).
```

## What it does

A client connects over TCP, subscribes to exact topic names or MQTT-style topic filters, and publishes a byte payload. The broker routes the publish to every currently connected matching subscriber. The implementation includes:

- MQTT 3.1.1 wire framing and a hand-written packet codec.
- A TCP listener, one connection handler per accepted socket, and a dedicated writer thread per connection.
- Four independent broker-registry actor shards by default.
- Exact topic fan-out plus `+` and `#` wildcard-filter fan-out.
- QoS 0 and a deliberately limited QoS 1 publisher acknowledgement path.
- Bounded outbound queues with a documented drop-oldest policy.
- Basic timestamped stdout/stderr logging and a small `--host`/`--port` CLI.

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

- `--host <address>` changes the bind address.
- `--port <u16>` changes the port.
- An unrecognized argument is logged and ignored.
- A missing or non-numeric value after `--port` is ignored and leaves the current/default port in place; it is not treated as a hard CLI error.
- A bind failure is logged and exits the process with status 1.

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

## Topic names and wildcard filters

Published topic names must be non-empty and cannot contain `+` or `#`. Subscription and unsubscription filters are validated before being sent to the broker:

- `+` must occupy an entire topic level, such as `sensors/+/temp`.
- `#` must occupy an entire final topic level, such as `sensors/#` or simply `#`.
- Empty filters, `sport#`, `sport/#/rank`, and `sport+` are rejected.
- Matching is iterative rather than recursive, including for adversarial slash-heavy strings.
- A matching `+` consumes exactly one level; a final `#` consumes zero or more remaining levels.

Exact subscriptions are hash-routed to one shard. Wildcard subscriptions are broadcast to every shard because a concrete topic and the matching wildcard string generally hash differently. During delivery, the publish shard uses an exact-match fast path, then checks wildcard filters, and tracks already-notified connection IDs so a client subscribed both exactly and through a matching wildcard does not receive the same publish twice.

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
- Every shard keeps a client entry for every registered connection, even if the connection has no subscription on that shard. This is intentional replication for routing and cleanup.
- The number of shards is a compile-time constant (`NUM_BROKER_SHARDS = 4`), not a CLI option.
- This project has not established throughput, latency, capacity, or fairness benchmarks. Do not infer a performance claim from the sharded design.

## Connection lifecycle and queue policy

For each accepted TCP socket, the handler clones the stream, starts a writer thread, and reads/decodes on the handler thread. Decoded packets are drained from the read buffer until the next packet is incomplete. A malformed packet or socket read error logs a warning, sends a broker `Disconnect`, closes the outbound queue, and ends the handler.

Each connection has an outbound queue of 128 events. The queue is built from `VecDeque`, `Mutex`, `Condvar`, and `Arc`:

- Under capacity it behaves FIFO.
- At capacity, a new event removes the oldest queued event and then appends the new event.
- Producers do not block waiting for a slow subscriber; the trade-off is intentional message loss under overload.
- Closing the queue wakes a blocked writer. A writer exits after the queue is closed and drained, or after a socket write error.

This queue policy applies to broker-to-client outbound packets only. There is no durable queue, disk persistence, reconnect replay, delivery receipt accounting, or broker-wide backpressure mechanism.

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
- The RETAIN bit is parsed and carried when a publish is forwarded, but the broker does not store retained messages or replay one to a later subscriber.
- Outbound UTF-8 encoding truncates a string longer than 65,535 bytes to the representable prefix instead of returning an encode error. That avoids an indexing panic but is not a substitute for full oversize-output handling.
- The connection handler does not enforce an MQTT state machine such as CONNECT-first. It dispatches supported packets as they arrive; well-behaved clients should still CONNECT before subscribing or publishing.

## Security and operational limits

- Transport is plaintext TCP only. TLS is intentionally excluded: Rust `std` does not provide the cryptographic/TLS primitives needed to implement TLS safely from scratch.
- No username/password authentication, ACLs, authorization, or tenant isolation exists.
- No persistent sessions, session takeover handling for duplicate MQTT client IDs, or offline-message storage exists. Internally, connections are keyed by a monotonically allocated local `AtomicU64` ID rather than the MQTT client ID.
- No last-will messages, retained-message store, or keep-alive timeout enforcement exists.
- A client that sends an unsupported feature or malformed input is disconnected; there is no MQTT error-response framework beyond the packet replies listed above.
- The broker is not an MQTT client and does not connect to upstream/external brokers.
- This is not a hardened internet-facing service. Put it behind appropriate network controls if used outside local development.

## Scope completion ledger

| PLAN.md extra item | Status | Detail |
|---|---|---|
| Topic wildcards | Completed | Validation, matching, broker fan-out, de-duplication, and cross-shard broadcast routing are implemented. |
| QoS 1 / `PUBACK` | Partially completed | Publisher receives a matching PUBACK; subscriber acknowledgement tracking, retry, redelivery, and DUP semantics are absent. |
| Sharded actor registry | Completed | Four hash-routed single-threaded actor shards are used by production startup. |
| Retained messages | Skipped | RETAIN is not a store/replay feature. |
| Last-will messages | Skipped | CONNECT with a will flag is rejected. |
| Keep-alive enforcement | Skipped | Keep-alive is parsed but not used for timeout/disconnect. |
| Reproducible build | **Achieved** | Two isolated release builds from **different directory paths** under rustc 1.97.1 produced byte-identical SHA-256 hashes. Toolchain pinned via `rust-toolchain.toml`; paths remapped via `.cargo/config.toml`; `codegen-units=1`, `debug=false` in `[profile.release]`. Proof in `proof/reproducible-build.md`. |

Out of scope: QoS 2, MQTT 5.0, persistent sessions, authentication/ACLs, encrypted transport, and a complete MQTT compliance implementation.

## Verification actually performed

The following were personally run against this worktree on 2026-08-31:

- `cargo test` completed with **77 passed, 0 failed**.
- The release build completed successfully (with non-fatal dead-code warnings in the current source).
- `python tests/interop/paho_client.py` passed: a paho-mqtt publisher and subscriber exchanged a byte-exact QoS 0 payload through the release broker.
- A separate live paho MQTT 3.1.1 check passed: a QoS 1 publisher received its PUBACK, and a subscriber on `role-d/+/temp` received the publish to `role-d/kitchen/temp` at QoS 1.
- `cargo tree --edges normal` listed only `blitzbroker v0.1.0` (the `.cmd` wrapper in `scripts/` reproduces this same command on Windows; it is not required to run it).

The automated test suite includes protocol, broker, connection, and queue coverage. Its named coverage includes malformed/truncated/unknown packet handling; Remaining Length boundaries; UTF-8 validation; PUBLISH QoS 0/1 and PUBACK validation; topic-filter validation/matching; disconnect cleanup; bounded queue behavior; drop-oldest stress accounting; exact fan-out; shard isolation; wildcard delivery/unsubscribe; and QoS1 PUBACK packet-ID matching.

Not claimed as verified here:

- No live TCP stress/throughput/latency benchmark.
- No complete interoperability matrix or MQTT-compliance certification.
- No direct Role D execution of the Mosquitto shell script in the current workspace.
- No demo video.

## Dependency and build evidence

The dependency-proof command is `cargo tree --edges normal`, run directly:

```bash
cargo tree --edges normal
```

Expected output (path differs per checkout, everything else is identical):

```text
blitzbroker v0.1.0 (/path/to/checkout)
```

A saved copy of this output is committed at [`proof/cargo-tree.txt`](proof/cargo-tree.txt).

`scripts/dependency-proof.cmd` is a one-line Windows wrapper around that same command kept for convenience; judges on any OS should run `cargo tree --edges normal` directly.

## Reproducible build evidence

Reproducible builds are achieved via three mechanisms:

1. **`rust-toolchain.toml`** — pins `rustc 1.97.1`; `rustup` installs it automatically.
2. **`.cargo/config.toml`** — `--remap-path-prefix` rewrites all absolute checkout paths to `blitzbroker/` before they are embedded in the binary.
3. **`[profile.release]`** in `Cargo.toml` — `codegen-units=1` (single deterministic CGU) and `debug=false` (no debug-info section, belt-and-suspenders path stripping).

Two builds from **different directory paths** produced byte-identical SHA-256 hashes. Full proof (hash, method, reproduce-yourself command) is in [`proof/reproducible-build.md`](proof/reproducible-build.md).

`STDLIB.md` documents the standard-library replacements for async/networking, registries, queueing, CLI parsing, logging, packet encoding, QoS1/PUBACK, wildcard matching, and connection IDs.

## Known gaps at submission time

This section exists so a judge does not have to find these out the hard way. As of the last audit (2026-08-31):

- **No OSI license file is present.** The event rules require a public repo with an OSI-approved license; without a `LICENSE` file this requirement is not currently met.
- **No demo video is present.** The 5-minute demo video is a required deliverable and has not been recorded/attached as of this writing.
- **`proof/cargo-tree.txt` is now committed** alongside the command that produces it.
- **`Personal_Decisions.md` is cited but missing.** Older internal logs and source comments reference a `Personal_Decisions.md` for several engineering rationales (decisions 1, 3A, 3B, 4, and 5 by that document's numbering). It is not present in this worktree or in any recoverable branch/reflog/stash history. That rationale has not been reconstructed or invented to fill the gap — DECISIONS.md #10 is the authoritative record of this specific documentation loss, and it should be read alongside any surviving reference to `Personal_Decisions.md` elsewhere in this repo.

See `DECISIONS.md` for the chronological engineering-decision record (including corrections logged against earlier entries), `STDLIB.md` for the zero-dependency substitutions, `AI_GUARDRAILS.md` for project safety rules, and `SUBMISSION_CHECKLIST.md` for the checklist snapshot this section summarizes.