# Personal Log — Role C: Testing, Interop & Fuzzing

**Project:** BlitzBroker
**Owner:** Member C

## Scope (from PLAN.md §5)
Unit test suite for parsing edge cases, interop scripts against real mosquitto/paho-mqtt clients, multi-client concurrency/stress test — the project's headline verification work.

## Task queue
- [ ] Unit tests: packet parsing edge cases (truncated, invalid remaining-length, oversized payload)
- [ ] Interop test: mosquitto_pub/mosquitto_sub against the broker
- [ ] Interop test: paho-mqtt (Python) scripted client against the broker
- [ ] Integration test: multi-client pub/sub fan-out correctness
- [ ] Integration test: disconnect cleanup (no leaked subscriptions)
- [ ] Stress test: N concurrent clients, M topics — verify no data loss beyond documented drop-oldest backpressure behavior
- [ ] Write up verification results for README (the provable-correctness story)

## Log
_Add dated entries below as you go — what you did, decisions made, blockers hit._
