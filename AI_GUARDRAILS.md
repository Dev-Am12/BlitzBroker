# AI_GUARDRAILS.md — BlitzBroker

Rules for any coding agent (or human contributor) working on this project. If generated output conflicts with this document, this document takes precedence.

## Non-negotiables

1. Zero third-party runtime dependencies, no exceptions. Do not add any crate to `Cargo.toml`. Do not write code that assumes a crate is available. This includes `tokio`, `serde`, `clap`, `log`, `crossbeam`, `mio`, and all others. `std` only.
2. No vendoring. Do not copy a crate's source into the project to fake an empty manifest. If `std` genuinely lacks something needed, implement it from the relevant public specification and record the substitution in `STDLIB.md`.
3. No panics on untrusted input. Every packet parser must handle malformed, truncated, or oversized input by returning an error — never `.unwrap()`/`.expect()`/indexing panics on bytes read from the network.
4. Packet-parsing code must be verifiable against the MQTT 3.1.1 specification section it implements. Do not rely on memorized or assumed wire-format details — check them against the spec text directly.
5. Every generated change must be reviewed by a human before merging. Do not merge unreviewed output regardless of time pressure.
6. Documentation must state only verified facts. Do not include performance, compliance, or interoperability claims in `README.md` or `STDLIB.md` that have not actually been run and confirmed.
7. Report technical decisions. If completing a task requires choosing between multiple reasonable approaches — not just following the prompt's explicit instructions or an existing established pattern in the code — state what was chosen and why in the summary. This is for the team to review and decide whether it belongs in `DECISIONS.md`; do not edit `DECISIONS.md` directly. Routine choices already dictated by the prompt or by matching an existing pattern elsewhere in the codebase don't need flagging — only points where a real judgment call was made.

## Agent-usage discipline

Agent usage draws from a limited free-tier quota per contributor. To use it efficiently:

- Reserve higher-capability model calls for correctness-critical work: packet framing/parsing (especially remaining-length encoding), the broker actor's channel wiring, and backpressure queue logic.
- Use lower-capability/faster model calls for boilerplate: CLI argument parsing, logging setup, test scaffolding, documentation drafts, and demo/test client scripts.
- Prompt with a complete, spec-referenced request — cite the exact MQTT packet type and byte layout — rather than iterating exploratively. This produces more correct output on the first pass and uses less quota.
- Commit working state before any large generated change, especially refactors, so a bad generation can be reverted rather than repaired.
- If a contributor's quota is exhausted, they should shift to reviewing or testing another module rather than remain idle.

## Definition of done

A task is complete when: it builds, it has at least one test covering its primary failure mode, — if it replaced something a third-party package would normally provide — `STDLIB.md` has a corresponding entry, and — if it's tracked in a personal log (`logs/role-*.md`) — the corresponding task-queue checkbox is marked done and a brief dated log entry is added describing what was done.