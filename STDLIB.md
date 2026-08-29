# STDLIB.md — BlitzBroker

Every package we'd normally install, and the std feature we used instead. **Owner: Role D compiles/enforces this, but whoever makes a substitution adds the entry the same day — don't reconstruct this at the end.**

Format per entry: `Package we'd normally use → std feature used instead — one-line rationale`

## Seeded entries (known from the plan, fill in specifics as implemented)

- `tokio` → `std::thread` + `std::sync::mpsc` — thread-per-connection + actor-model broker thread instead of an async runtime; see DECISIONS.md for the concurrency trade-off.
- `serde` (+ a hand-written MQTT codec crate) → hand-rolled packet encode/decode per MQTT 3.1.1 spec — no JSON/serialization framework needed since the wire format is fixed-layout binary, not a generic serialization problem.
- `clap` → hand-rolled `std::env::args()` parsing — CLI surface is small (host/port), didn't justify a parsing framework.
- `log` / `tracing` → hand-rolled leveled logger over `std::time` + stdout — no need for structured/async logging at this scale.
- `dashmap` / a concurrent-hashmap crate → **not needed at all**, and that's itself the point: the actor model routes every registry mutation through one owning thread, so there's no concurrent-map problem to solve in the first place.
- `crossbeam` (bounded channel/queue utilities) → hand-rolled bounded queue with drop-oldest policy over `std::collections::VecDeque` — small enough to implement and test directly, and the drop-oldest policy is a deliberate design choice worth owning rather than inheriting a general-purpose library's defaults.

## Add entries below as work progresses

_(date, entry, who added it)_
