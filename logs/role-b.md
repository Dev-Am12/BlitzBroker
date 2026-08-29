# Personal Log — Role B: Protocol & Packet Parsing

**Project:** BlitzBroker
**Owner:** Member B

## Scope (from PLAN.md §5)
Fixed/variable header parsing, remaining-length encoding/decoding, all packet-type encode/decode logic, malformed-input rejection paths.

## Task queue
- [ ] Remaining-length variable encoding/decoding (MQTT spec)
- [ ] Fixed header parsing (packet type, flags)
- [ ] CONNECT / CONNACK
- [ ] SUBSCRIBE / SUBACK, UNSUBSCRIBE / UNSUBACK
- [ ] PUBLISH (QoS 0)
- [ ] PINGREQ / PINGRESP, DISCONNECT
- [ ] Malformed/truncated/oversized-input rejection paths for every packet type (cite spec section for each)
- [ ] (stretch) Topic wildcards (+, #)
- [ ] (stretch) QoS 1 (PUBACK)

## Log
_Add dated entries below as you go — what you did, decisions made, blockers hit._
