# Personal Log — Role A: Broker Core & Concurrency

**Project:** BlitzBroker
**Owner:** Member A

## Scope (from PLAN.md §5)
TCP listener/accept loop, connection-handler threads, broker actor thread, mpsc channel wiring, per-client outbound queue + backpressure/drop-oldest logic, disconnect cleanup.

## Task queue
- [x] TCP listener + accept loop, thread-per-connection
- [x] Broker actor thread skeleton + mpsc channel types (Subscribe/Unsubscribe/Publish/Disconnect messages)
- [x] Topic → subscriber registry (owned exclusively by broker thread) — exact-match, core scope
- [x] Per-client bounded outbound queue, drop-oldest policy — tested (queue.rs)
- [x] Fan-out logic on Publish — core scope
- [x] Disconnect/error cleanup (remove client from all subscriptions) — tested
- [x] SUBACK/UNSUBACK response wiring (connection.rs) — closed a core-scope gap found during verification
- [x] (stretch) Sharded-actor upgrade by topic hash — DECISIONS.md #8. `ShardedBroker` routes exact-match traffic by topic hash across N independent single-threaded shards; Register/Disconnect broadcast to all shards. Live-verified: exact-match and QoS1 regressions clean across the sharded broker, no correctness loss from the single-shard design.
- [x] blitzclient — standalone pub/sub client binary, DECISIONS.md #10. No shared code with the broker (self-contained codec, ~5 packet types). Live cross-interop verified in both directions against real mosquitto clients, not just against our own broker.
- [x] Retained messages (PLAN.md §4 item 4) — per-shard retained-message store in `run_broker`, delivered on Subscribe (exact and wildcard), empty-payload clears. Live-verified: basic retain, overwrite-keeps-latest, empty-payload-clear, and an 8-topic cross-shard wildcard-retained-delivery check (deliberately more thorough than the automated test's single pair).
- [ ] Last-will messages (PLAN.md §4 item 5) — not started
- [ ] Keep-alive timeout enforcement (PLAN.md §4 item 6) — not started
- [ ] Logging polish (human-readable timestamps, level filtering) — not started

## Status
All Role A queue items through retained messages are complete and independently live-verified (not just unit-tested) — real mosquitto clients throughout, plus blitzclient's own cross-interop against mosquitto in both directions. Last-will, keep-alive, and logging polish are queued but not yet started (paused for time budget reasons).

## Log
_Add dated entries below as you go — what you did, decisions made, blockers hit._
- Core skeleton (listener, broker actor, queue, connection handling) built and unit-tested.
- Verified live against real mosquitto_pub/mosquitto_sub clients — exact-match pub/sub confirmed working end to end.
- Closed the SUBACK/UNSUBACK core-scope gap found during verification (connection.rs) — 55/55 tests passing, no technical decisions flagged for DECISIONS.md review.
- Sharded-actor upgrade (stretch item) implemented and verified: N-shard routing by topic hash, Register/Disconnect broadcast to all shards. 72/72 tests passing at that point; no regression in exact-match or QoS1 behavior.
- After Member B's wildcard-fan-out fix landed on top of sharding (which required extending the broadcast treatment to wildcard Subscribe/Unsubscribe — see DECISIONS.md #9), independently live-verified: 3 different `+`-filter/topic pairs across shards, one `#`-filter deep-nested match, one negative (non-matching) case, plus QoS0/QoS1 regressions. All correct. 77/77 automated tests passing.
- blitzclient (pub + sub subcommands) implemented and live cross-interop tested against real mosquitto clients in both directions, in addition to unit tests.
- Retained messages implemented: per-shard store, delivered on subscribe (exact and wildcard), empty-payload clears. Live-verified beyond the automated test suite: 8 different retained topics across shards, single wildcard subscribe, all 8 delivered correctly. 118/118 automated tests passing (82 broker + 36 client) at this point. Paused here — last-will, keep-alive enforcement, and logging polish not yet started.