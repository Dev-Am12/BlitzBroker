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

## Status
All Role A queue items, including the stretch item, are complete and independently live-verified (not just unit-tested) — real mosquitto clients, multiple topic pairs, exact-match and QoS1 both confirmed working through the sharded broker.

Two things surfaced during this round that aren't Role A's to fix, but are worth tracking: `logs/role-c-verification.md`'s limitations table is now stale (claims QoS1/wildcard-live are untested — both now confirmed working, see DECISIONS.md #7 and #9), and several files reference a `Personal_Decisions.md` that doesn't exist in the repo.

## Log
_Add dated entries below as you go — what you did, decisions made, blockers hit._
- Core skeleton (listener, broker actor, queue, connection handling) built and unit-tested.
- Verified live against real mosquitto_pub/mosquitto_sub clients — exact-match pub/sub confirmed working end to end.
- Closed the SUBACK/UNSUBACK core-scope gap found during verification (connection.rs) — 55/55 tests passing, no technical decisions flagged for DECISIONS.md review.
- Sharded-actor upgrade (stretch item) implemented and verified: N-shard routing by topic hash, Register/Disconnect broadcast to all shards. 72/72 tests passing at that point; no regression in exact-match or QoS1 behavior.
- After Member B's wildcard-fan-out fix landed on top of sharding (which required extending the broadcast treatment to wildcard Subscribe/Unsubscribe — see DECISIONS.md #9), independently live-verified: 3 different `+`-filter/topic pairs across shards, one `#`-filter deep-nested match, one negative (non-matching) case, plus QoS0/QoS1 regressions. All correct. 77/77 automated tests passing.