# Submission Checklist

Audited by Role D on 2026-08-31 against the current worktree.

- [ ] Public GitHub repository, OSI license — the configured `origin` is a GitHub URL, but no `LICENSE`/`COPYING` file is present, so this combined requirement is not complete.
- [x] One-command build producing a runnable artifact — `cargo build --release` completed successfully; it produces `target/release/blitzbroker.exe`.
- [x] Empty `Cargo.toml` `[dependencies]` — inspected directly; `Cargo.lock` contains only the local `blitzbroker` package.
- [x] Dependency-proof output — [`proof/cargo-tree.txt`](proof/cargo-tree.txt) is the saved output of `cargo tree --edges normal` against this manifest/lockfile.
- [x] README — [`README.md`](README.md) documents operation, verified features, and limits.
- [x] STDLIB — [`STDLIB.md`](STDLIB.md) was cross-checked against the implemented std-only substitutions.
- [ ] Five-minute demo video — no video artifact is present in the repository.

