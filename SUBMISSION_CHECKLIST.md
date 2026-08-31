# Submission Checklist

Audited by Role D on 2026-08-31 against the current worktree.

- [x] Public GitHub repository, OSI license — `LICENSE` file is present (MIT). Repository is public on GitHub.
- [x] One-command build producing a runnable artifact — `cargo build --release` completed successfully; it produces `target/release/blitzbroker.exe`.
- [x] Empty `Cargo.toml` `[dependencies]` — inspected directly; `Cargo.lock` contains only the local `blitzbroker` package.
- [x] Dependency-proof output — [`proof/cargo-tree.txt`](proof/cargo-tree.txt) is the saved output of `cargo tree --edges normal` against this manifest/lockfile.
- [x] README — [`README.md`](README.md) documents operation, verified features, and limits.
- [x] STDLIB — [`STDLIB.md`](STDLIB.md) was cross-checked against the implemented std-only substitutions.
- [x] Reproducible build (+5 bonus) — two consecutive clean builds and two builds from different directory paths under rustc 1.97.1 (pinned via `rust-toolchain.toml`) produced SHA-256 `4D62CFB48F1B3377A7AC24C302E5FCF5D63604BAC89DBB5D0C7FE78822FF0278`. Fixed via `-C link-arg=/Brepro` (MSVC PE timestamp) + `--remap-path-prefix` + `codegen-units=1`. Proof in [`proof/reproducible-build.md`](proof/reproducible-build.md).
- [ ] Five-minute demo video — no video artifact is present in the repository.



