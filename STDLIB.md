# STDLIB.md — BlitzBroker

Every package we'd normally install, and the std feature we used instead. **Owner: Role D compiles/enforces this, but whoever makes a substitution adds the entry the same day.** Entries below were cross-checked against `protocol.rs`, `broker.rs`, `connection.rs`, `queue.rs`, and `logging.rs` on 2026-08-31.

Format per entry: `Package we'd normally use → std feature used instead — one-line rationale`

## Implemented substitutions

- `tokio` / `mio` → `std::net::{TcpListener, TcpStream}`, `std::thread`, and `std::sync::mpsc` — one connection handler per TCP connection and actor-message routing without an async runtime (DECISIONS.md #1, #8).
- `dashmap` / concurrent-map crates → shard-owned `std::collections::HashMap` — registry state has one owner per shard; `ShardedBroker` uses `std::sync::Arc`, `mpsc::Sender`, and `DefaultHasher` to route exact topics and broadcast wildcard subscriptions (DECISIONS.md #8, #9).
- `crossbeam` / `flume` bounded-channel utilities → hand-rolled `VecDeque` behind `std::sync::{Mutex, Condvar, Arc}` — the queue must drop the oldest buffered item instead of blocking the broker behind a slow subscriber (PLAN.md §3; `queue.rs`).
- `clap` → hand-rolled `std::env::args()` parsing — the CLI only accepts `--host` and `--port` (`main.rs`).
- `log` / `tracing` → small logger using `std::time::{SystemTime, UNIX_EPOCH}` and stdout/stderr — timestamped levelled output is sufficient for this broker (`logging.rs`).
- `serde`, `bytes`, or an MQTT codec crate such as `mqttbytes` → hand-rolled MQTT 3.1.1 packet codec using `Vec<u8>`, byte slices, `std::str`, and checked lengths — the broker needs a fixed binary wire format, including malformed-input rejection, not general-purpose serialization (`protocol.rs`).
- MQTT QoS helpers from a broker/client crate → explicit `PUBLISH` QoS 0/1 and `PUBACK` encode/decode plus connection dispatch — QoS 1 publisher acknowledgements are implemented without a protocol dependency; subscriber pending-ack tracking/retry is intentionally absent (DECISIONS.md #6, #7).
- MQTT topic-filter helpers → iterative `str::split`-based validation and matching — `+` and `#` filter support is implemented locally and the broker performs a de-duplicated wildcard fan-out pass (DECISIONS.md #5, #9).
- `uuid` / connection-ID helpers → `std::sync::atomic::AtomicU64` — locally unique connection IDs avoid an additional identifier dependency; MQTT client-ID session collision handling remains out of scope (`broker.rs`).

Developer-only paho-mqtt and mosquitto tools in `tests/interop/` are not Rust runtime dependencies and are not listed in `Cargo.toml`/`Cargo.lock`.

- `proptest` / `cargo-fuzz` → hand-rolled adversarial byte sequences in `cargo test` (e.g., `decode_never_panics_on_random_bytes` and max `remaining_length` tests) — fuzz-guarding is achieved via targeted edge-case unit tests rather than injecting an external property-testing or fuzzing framework dependency.

- `assert_cmd` / Rust test-client crates (e.g., `rumqttc`) → standalone Python and Bash scripts (`paho_client.py`, `mosquitto.sh`) — end-to-end integration and interop are verified via system subprocesses driving standard tools, keeping the Rust test suite strictly self-contained and free of test-only external crates.