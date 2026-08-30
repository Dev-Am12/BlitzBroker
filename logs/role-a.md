# Personal Log — Role A: Broker Core & Concurrency

**Project:** BlitzBroker
**Owner:** Member A

## Scope (from PLAN.md §5)
TCP listener/accept loop, connection-handler threads, broker actor thread, mpsc channel wiring, per-client outbound queue + backpressure/drop-oldest logic, disconnect cleanup.

## Task queue
- [x] TCP listener + accept loop, thread-per-connection
- [x] Broker actor thread skeleton + mpsc channel types (Subscribe/Unsubscribe/Publish/Disconnect messages)
- [x] Topic → subscriber registry (owned exclusively by broker thread) — exact-match (core scope). Wildcard-aware matching is a separate extra-scope item, being done by Member B in the same file (see DECISIONS.md #8).
- [x] Per-client bounded outbound queue, drop-oldest policy — tested (queue.rs)
- [x] Fan-out logic on Publish — core scope (exact-match)
- [x] Disconnect/error cleanup (remove client from all subscriptions) — tested
- [x] SUBACK/UNSUBACK response wiring (connection.rs) — not on the original queue, added here since it's Role A-scoped file work; closed a core-scope gap found during verification. 55/55 tests passing after this landed.
- [ ] (stretch) Sharded-actor upgrade by topic hash — not started

## Status
All core-scope Role A items are complete. The only thing remaining on this queue is the stretch item (sharded-actor upgrade), which is extra scope, not required for a strong submission.

## Log
_Add dated entries below as you go — what you did, decisions made, blockers hit._
- Core skeleton (listener, broker actor, queue, connection handling) built and unit-tested.
- Verified live against real mosquitto_pub/mosquitto_sub clients — exact-match pub/sub confirmed working end to end.
- Closed the SUBACK/UNSUBACK core-scope gap found during verification (connection.rs) — 55/55 tests passing, no technical decisions flagged for DECISIONS.md review (implementation followed the prompt's explicit spec throughout).