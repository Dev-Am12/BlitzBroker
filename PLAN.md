# PLAN.md — BlitzBroker (Zero Dependency 2026, Track C)

**Project name:** BlitzBroker
**Description:** A from-scratch, zero-dependency MQTT 3.1.1-subset broker in Rust, verified against real off-the-shelf MQTT clients.
**Track:** C — Web & Network. **Language:** Rust, `std` only.
**Target:** 1st place. Event window: kickoff Aug 28 23:30 IST — code freeze Aug 31 23:30 IST (18:00 UTC).
**Written:** Aug 29, 2026, ~18h40m after kickoff (~53h remaining to code freeze) — fixed reference point for the milestones in §6.

---

## 1. What this project builds

A TCP server speaking a real, documented subset of the MQTT 3.1.1 wire protocol. Clients `CONNECT`, `SUBSCRIBE` to named topics, `PUBLISH` messages, and the broker fans each published message out to every current subscriber of that topic, in real time, with no third-party crates anywhere in the shipped artifact.

Key design choice: the protocol subset implemented is a real, published spec (not a fully custom design), so genuine off-the-shelf MQTT clients (mosquitto CLI tools, `paho-mqtt`) can connect to and interoperate with this broker. This produces an external, provable correctness story instead of a self-verified one.

---

## 2. Concurrency architecture (full reasoning in DECISIONS.md)

**Actor model.** One dedicated broker thread owns the topic → subscriber registry exclusively. Every client connection runs on its own thread (accept loop spawns a thread per connection), and that thread never touches the registry directly — it only sends messages (`Subscribe`, `Unsubscribe`, `Publish`, `Disconnect`) to the broker thread over an `std::sync::mpsc` channel. The broker thread processes these serially and, for each `Publish`, looks up subscribers and pushes the message onto each subscriber's own outbound queue.

Guarantees this gives, to be stated explicitly in README.md:
- No data races on the registry (only one thread ever mutates it).
- Per-topic publish ordering is preserved (single-threaded serial processing).

Known, accepted limitation, to be documented rather than hidden: a single broker thread is a serialization point — under very high message rates across many topics, throughput is bounded by that one thread. The sharded-actor concurrency upgrade module (§4) is the scoped fix if pursued.

---

## 3. Core scope — MUST DO

A working, demoable broker with:

- **Transport:** raw TCP via `std::net::TcpListener`/`TcpStream`. Thread-per-connection accept loop.
- **Packet types implemented** (MQTT 3.1.1 subset):
  - `CONNECT` / `CONNACK` (client ID, clean-session flag; no persistent sessions in core)
  - `SUBSCRIBE` / `SUBACK` (exact topic match only — no wildcards in core)
  - `UNSUBSCRIBE` / `UNSUBACK`
  - `PUBLISH` at **QoS 0 only** (fire-and-forget, no PUBACK)
  - `PINGREQ` / `PINGRESP`
  - `DISCONNECT`
- **Wire-format correctness:** fixed header, MQTT variable-length "remaining length" encoding, variable header and payload per packet type, all per MQTT 3.1.1 spec section-by-section.
- **Malformed-input handling:** truncated packets, invalid remaining-length encoding, oversized payloads — all rejected cleanly, never a panic on untrusted bytes.
- **Broker registry & fan-out:** topic → list of subscriber channels; publish delivers to every current subscriber of that exact topic.
- **Backpressure:** bounded per-client outbound queue (fixed capacity), **drop-oldest** policy when full, documented as the explicit chosen strategy.
- **Clean disconnect handling:** a client disconnecting or erroring out is removed from every topic it was subscribed to, with no leaks.
- **CLI:** host/port configuration via hand-rolled `std::env::args()` parsing.
- **Logging:** hand-rolled leveled logger (timestamped stdout output via `std::time`).
- **Interop verification:** real `mosquitto_pub`/`mosquitto_sub` (and/or a `paho-mqtt` Python script) connected against the broker, demonstrating working pub/sub — the project's headline correctness claim.
- **Tests:** unit tests for packet parsing (including malformed/truncated/edge-case input), integration test for multi-client pub/sub fan-out and disconnect cleanup.
- **Submission artifacts:** `README.md`, `STDLIB.md`, dependency-proof script/output, empty `Cargo.toml` `[dependencies]`.

This is the acceptance bar for a strong submission even if none of the extra-scope items below are completed.

---

## 4. Extra scope — IN PRIORITY ORDER, attempt only after core is fully done and tested

1. **Topic wildcards** (`+` single-level, `#` multi-level) — real MQTT feature, meaningful craft/innovation value.
2. **QoS 1** (at-least-once, `PUBACK`) — natural next correctness milestone once QoS 0 is solid.
3. **Sharded-actor concurrency upgrade module** — shard the registry by topic hash into N broker threads instead of one. Extends the existing design rather than replacing it (each topic maps deterministically to one shard, no cross-shard coordination needed). A before/after benchmark, if reached, is strong supporting material.
4. **Retained messages** — a new subscriber immediately receives the last retained message on a topic.
5. **Last-will messages** — published automatically on ungraceful disconnect.
6. **Keep-alive timeout enforcement** — disconnect clients that go silent past their declared keep-alive interval.
7. **Reproducible build** — pin toolchain version, verify byte-identical output across two builds.
8. **Single File merge** — see §6.

**Out of scope, not attempted:**
- QoS 2 (exactly-once), full MQTT 5.0, persistent sessions across reconnects, authentication/ACL beyond passthrough fields — excluded for time/scope reasons; may move into extra scope later if time allows.
- **TLS / encrypted transport — excluded structurally, not as a time-permitting stretch item.** `std` provides no crypto primitives to build TLS on safely; this is not something more time would resolve within this project's constraints. See DECISIONS.md.

---

## 5. Role breakdown (4 contributors)

Roles, not silos — cross-review is required per `AI_GUARDRAILS.md` regardless of who wrote what.

- **Role A — Broker Core & Concurrency:** TCP listener/accept loop, connection-handler threads, broker actor thread, `mpsc` channel wiring, per-client outbound queue + backpressure/drop-oldest logic, disconnect cleanup.
- **Role B — Protocol & Packet Parsing:** fixed/variable header parsing, remaining-length encoding/decoding, all packet-type encode/decode logic, malformed-input rejection paths.
- **Role C — Testing, Interop & Fuzzing:** unit test suite for parsing edge cases, interop scripts against real `mosquitto`/`paho-mqtt` clients, multi-client concurrency/stress test — the project's headline verification work.
- **Role D — Docs, Build & Submission:** `STDLIB.md` maintenance (compiles and enforces entries as substitutions land), `README.md`, `DECISIONS.md` upkeep, dependency-proof script, demo video, reproducible-build setup if attempted.

Personal logs: `logs/role-a.md`, `logs/role-b.md`, `logs/role-c.md`, `logs/role-d.md` — update the Owner field once roles are assigned.

---

## 6. Milestones (anchored to elapsed time from this document's write time, IST)

| Checkpoint | Target time (approx) | Expected state |
|---|---|---|
| +8h | ~Aug 30, 02:00 IST | Core skeleton compiles: listener + accept loop + broker actor thread stub wired via `mpsc`, CLI, logging skeleton |
| +20h | ~Aug 30, 14:00 IST | CONNECT/CONNACK, SUBSCRIBE/SUBACK, PUBLISH(QoS0), PING, DISCONNECT working end-to-end for a single client; multi-client fan-out working |
| +30h | ~Aug 31, 00:00 IST | Backpressure implemented & tested, disconnect cleanup solid, malformed-input handling hardened, first real interop test against mosquitto passes |
| +38h | ~Aug 31, 08:00 IST | Core feature-complete, all core tests green, STDLIB.md core entries done, README draft written |
| +45h | ~Aug 31, 15:00 IST | Extra-scope items attempted in priority order as time allows; single-file rehearsal merge happens here, not later |
| +50h | ~Aug 31, 20:00 IST | Final single-file merge (if pursued), reproducible-build check, feature work frozen |
| +53h (deadline) | Aug 31, 23:30 IST | Demo video recorded, docs polished, submission checklist verified |

---

## 7. Submission checklist (from event rules)

- [ ] Public GitHub repo, OSI license
- [ ] One-command build producing a runnable artifact
- [ ] Empty `Cargo.toml` `[dependencies]`
- [ ] Dependency-proof output (command output or CI log showing zero third-party deps)
- [ ] README.md — what it does, how to run, honest limits
- [ ] STDLIB.md — every substitution, with rationale
- [ ] 5-minute demo video — working tool + empty manifest shown on screen
