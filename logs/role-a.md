# Personal Log — Role A: Broker Core & Concurrency

**Project:** BlitzBroker
**Owner:** Member A

## Scope (from PLAN.md §5)
TCP listener/accept loop, connection-handler threads, broker actor thread, mpsc channel wiring, per-client outbound queue + backpressure/drop-oldest logic, disconnect cleanup.

## Task queue
- [ ] TCP listener + accept loop, thread-per-connection
- [ ] Broker actor thread skeleton + mpsc channel types (Subscribe/Unsubscribe/Publish/Disconnect messages)
- [ ] Topic → subscriber registry (owned exclusively by broker thread)
- [ ] Per-client bounded outbound queue, drop-oldest policy
- [ ] Fan-out logic on Publish
- [ ] Disconnect/error cleanup (remove client from all subscriptions)
- [ ] (stretch) Sharded-actor upgrade by topic hash

## Log
_Add dated entries below as you go — what you did, decisions made, blockers hit._
