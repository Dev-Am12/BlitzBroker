# Personal Log — Role D: Docs, Build & Submission

**Project:** BlitzBroker
**Owner:** Member D

## Scope (from PLAN.md §5)
STDLIB.md maintenance (compiles and enforces entries from others), README.md, DECISIONS.md upkeep, dependency-proof script, demo video, reproducible-build setup if attempted.

## Task queue
- [x] Keep STDLIB.md current as substitutions land — chased and cross-checked entries against current source
- [x] Dependency-proof script/output (`cargo tree` or equivalent showing zero third-party deps)
- [x] README.md draft — what it does, how to run, honest limits (TLS out of scope, QoS0-only core, etc.)
- [x] Keep DECISIONS.md current as decisions get made
- [ ] (stretch) Reproducible build: pin toolchain, verify byte-identical output across two builds
- [ ] 5-minute demo video: build from empty manifest, run broker, real mosquitto/paho-mqtt client connecting live, manifest shown on screen
- [x] Submission checklist final pass (PLAN.md §7)

## Log
_Add dated entries below as you go — what you did, decisions made, blockers hit._

- 2026-08-31 — Audited the required project records and current source before README work. `cargo test` passed 77/77. The stale Role C limitations table was recoverable only from Git because its worktree file had been deleted; restored it and corrected the two conflicting rows. Current broker/connection tests cover QoS 1 publisher PUBACK and same-/cross-shard wildcard delivery. I additionally built release, reran the existing paho smoke test (PASS), and live-verified with paho that a QoS 1 publisher received PUBACK while `role-d/+/temp` received a publish to `role-d/kitchen/temp`.
- 2026-08-31 — Investigated the cited `Personal_Decisions.md`: it is absent from the worktree, every local/remote tracked branch, reflog-reachable history, available stash, and unreachable tree. The actual rationale cannot be recovered, so DECISIONS.md #10 explicitly records the missing source rather than inventing Decisions 1/3A/3B/4/5. Older citations remain an unresolved provenance warning.
- 2026-08-31 — Cross-checked `STDLIB.md` against the protocol, broker, connection, queue, and logging implementations; added concrete entries for QoS 1/PUBACK, wildcard matching, sharding, queue synchronization, codec bytes, and connection IDs. Saved actual `cargo tree --edges normal` output (only `blitzbroker`) and added a reproducible proof command.
- 2026-08-31 — Drafted README.md and audited PLAN.md §7 in SUBMISSION_CHECKLIST.md. Build, empty manifest, dependency proof, README, and STDLIB are personally checked. License/public-repo requirement remains incomplete because no OSI license file is present; no demo video artifact is present.
- 2026-08-31 — Attempted the reproducible-build stretch with two independent `cargo build --release` outputs under Rust 1.98.0 in separate target directories. The executable SHA-256 hashes differed; exact commands and hashes are saved in `proof/reproducible-build-check.md`. The item remains unchecked and README makes no reproducibility claim. No source change was made to try to force a match.

- 2026-08-31 — Final pass: `cargo test` again passed 77/77; the saved dependency-proof output exactly matched a fresh `cargo tree --edges normal`. PowerShell execution policy blocks the `.ps1` wrapper, so I replaced it with an equivalent `dependency-proof.cmd` wrapper. No video files or available ffmpeg/OBS recorder command were found, so the demo-video requirement remains explicitly unchecked rather than fabricated.

- 2026-08-31 — Expanded README.md into a source-backed technical reference. It now documents packet-level behavior, routing/sharding, queue semantics, parser boundaries, security/scope exclusions, actual verification, non-claims, and the current submission blockers. No source files were changed.
