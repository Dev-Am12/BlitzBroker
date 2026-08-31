# BlitzBroker

BlitzBroker is a from-scratch MQTT 3.1.1 **subset** broker written in Rust. This submission builds directly with `rustc` and uses the standard library only—there is no Cargo manifest and no third-party runtime dependency. It is intentionally a small, plaintext TCP broker—not a complete MQTT server and not a production security boundary.

**Track C — Web & Network.**

## Single-file submission

`blitzbroker.rs` is the complete, flattened broker submission: it has no Rust module declarations and builds directly with the included `Makefile`.

For the Single File bonus, `blitzbroker.rs` is the one-file broker artifact; `blitzclient.rs` is deliberately shipped as a separate, self-contained client binary.

## Quick start

```bash
# Build (Rust edition 2021)
make build

# Run
./blitzbroker --host 127.0.0.1 --port 1883
```

`deps-proof.txt` documents the dependency claim: this artifact has no Cargo manifest and builds directly with `rustc`, using the standard library only.

```bash
rustc --edition 2021 --test blitzbroker.rs -o blitzbroker-tests
./blitzbroker-tests
# 82 passed, 0 failed at submission verification.
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

- Rust/Cargo compatible with the project edition (`2021`). Verification for this submission was performed with rustc 1.98.0 on `x86_64-pc-windows-msvc`; no toolchain pin is committed to the repository. The code uses only `std::net`, `std::thread`, and `std::sync` — nothing platform-specific — so it is expected to build the same way on Linux and macOS, but that has not been independently verified on this worktree and is stated as an expectation, not a tested claim.
- A free TCP port. The default MQTT port is `1883`.

Build and start the broker in one command:

```bash
make build && ./blitzbroker --host 127.0.0.1 --port 1883
```

Build without starting it:

```bash
make build
```

The release executable is `blitzbroker.exe` on Windows, or `blitzbroker` on Linux/macOS. If `--host` or `--port` is omitted, the broker defaults to `127.0.0.1:1883`.

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
- A retained publish is stored per topic, replayed to later exact or wildcard subscribers, overwritten by a later retained publish, and cleared by an empty-payload retained publish.
- Outbound UTF-8 encoding truncates a string longer than 65,535 bytes to the representable prefix instead of returning an encode error. That avoids an indexing panic but is not a substitute for full oversize-output handling.
- The connection handler does not enforce an MQTT state machine such as CONNECT-first. It dispatches supported packets as they arrive; well-behaved clients should still CONNECT before subscribing or publishing.

## Security and operational limits

- Transport is plaintext TCP only. TLS is intentionally excluded: Rust `std` does not provide the cryptographic/TLS primitives needed to implement TLS safely from scratch.
- No username/password authentication, ACLs, authorization, or tenant isolation exists.
- No persistent sessions, session takeover handling for duplicate MQTT client IDs, or offline-message storage exists. Internally, connections are keyed by a monotonically allocated local `AtomicU64` ID rather than the MQTT client ID.
- No last-will messages or keep-alive timeout enforcement exists.
- A client that sends an unsupported feature or malformed input is disconnected; there is no MQTT error-response framework beyond the packet replies listed above.
- The broker is not an MQTT client and does not connect to upstream/external brokers.
- This is not a hardened internet-facing service. Put it behind appropriate network controls if used outside local development.

## Scope completion ledger

| PLAN.md extra item | Status | Detail |
|---|---|---|
| Topic wildcards | Completed | Validation, matching, broker fan-out, de-duplication, and cross-shard broadcast routing are implemented. |
| QoS 1 / `PUBACK` | Partially completed | Publisher receives a matching PUBACK; subscriber acknowledgement tracking, retry, redelivery, and DUP semantics are absent. |
| Sharded actor registry | Completed | Four hash-routed single-threaded actor shards are used by production startup. |
| Retained messages | Completed | A shard-owned retained store replays matching retained publishes to later exact and wildcard subscribers; empty retained payloads clear the stored message. |
| Last-will messages | Skipped | CONNECT with a will flag is rejected. |
| Keep-alive enforcement | Skipped | Keep-alive is parsed but not used for timeout/disconnect. |
| Reproducible build | Attempted, not achieved | Two isolated release builds under Rust 1.98.0 produced different executable SHA-256 hashes. No reproducibility claim is made. |

Out of scope: QoS 2, MQTT 5.0, persistent sessions, authentication/ACLs, encrypted transport, and a complete MQTT compliance implementation.

## Verification actually performed

The following were personally run against this worktree on 2026-08-31:

- Submission verification ran `cargo test` in a fresh project containing `blitzbroker.rs` as `src/main.rs`: **82 passed, 0 failed**.
- The fresh project's release build completed successfully with **zero warnings**.
- `python tests/interop/paho_client.py` passed: a paho-mqtt publisher and subscriber exchanged a byte-exact QoS 0 payload through the release broker.
- A separate live paho MQTT 3.1.1 check passed: a QoS 1 publisher received its PUBACK, and a subscriber on `role-d/+/temp` received the publish to `role-d/kitchen/temp` at QoS 1.
- `deps-proof.txt` records that this plain-`rustc` submission has no manifest and therefore no third-party dependency tree.

The automated test suite includes protocol, broker, connection, and queue coverage. Its named coverage includes malformed/truncated/unknown packet handling; Remaining Length boundaries; UTF-8 validation; PUBLISH QoS 0/1 and PUBACK validation; topic-filter validation/matching; disconnect cleanup; bounded queue behavior; drop-oldest stress accounting; exact fan-out; shard isolation; wildcard delivery/unsubscribe; and QoS1 PUBACK packet-ID matching.

Not claimed as verified here:

- No live TCP stress/throughput/latency benchmark.
- No complete interoperability matrix or MQTT-compliance certification.
- No direct Role D execution of the Mosquitto shell script in the current workspace.
- No demo video.

## Dependency and build evidence

This plain-`rustc` submission has no Cargo manifest. `deps-proof.txt` is the authoritative dependency proof for this artifact; the Cargo material below is retained only as historical evidence from the multi-file development worktree.

The dependency-proof command is `cargo tree --edges normal`, run directly:

```bash
cargo tree --edges normal
```

Expected current output (path will differ per checkout):

```text
blitzbroker v0.1.0 (C:\Hackathons\BlitzBroker)
```

`scripts/dependency-proof.cmd` is a one-line Windows wrapper around that same command, kept for convenience during Windows-based development; it is not required and has no Linux/macOS equivalent committed. Judges on any OS should run `cargo tree --edges normal` directly rather than the `.cmd` file.

**Known gap:** the submission checklist below references a saved `proof/cargo-tree.txt` output file. That file is not present in this worktree as of the last audit (2026-08-31) — only the command that produces it is committed. Treat the command above as the authoritative dependency proof until a static `proof/cargo-tree.txt` is committed alongside it.

`STDLIB.md` documents the standard-library replacements for async/networking, registries, queueing, CLI parsing, logging, packet encoding, QoS1/PUBACK, wildcard matching, and connection IDs.

## Known gaps at submission time

This section exists so a judge does not have to find these out the hard way. As of the last audit (2026-08-31):

- **No OSI license file is present.** The event rules require a public repo with an OSI-approved license; without a `LICENSE` file this requirement is not currently met.
- **No demo video is present.** The 5-minute demo video is a required deliverable and has not been recorded/attached as of this writing.
- **The Reproducible Build bonus (+5) was attempted and not achieved.** Two isolated release builds under rustc 1.98.0 produced different SHA-256 hashes for the output binary. No reproducibility claim is made; see the scope-completion ledger above.
- **`proof/cargo-tree.txt` is not committed** — see "Dependency and build evidence" above. The command that generates it is committed and works; the static output file is not yet saved.
- **`Personal_Decisions.md` is cited but missing.** Older internal logs and source comments reference a `Personal_Decisions.md` for several engineering rationales (decisions 1, 3A, 3B, 4, and 5 by that document's numbering). It is not present in this worktree or in any recoverable branch/reflog/stash history. That rationale has not been reconstructed or invented to fill the gap — DECISIONS.md #10 is the authoritative record of this specific documentation loss, and it should be read alongside any surviving reference to `Personal_Decisions.md` elsewhere in this repo.

See `DECISIONS.md` for the chronological engineering-decision record (including corrections logged against earlier entries), `STDLIB.md` for the zero-dependency substitutions, `AI_GUARDRAILS.md` for project safety rules, and `SUBMISSION_CHECKLIST.md` for the checklist snapshot this section summarizes.
