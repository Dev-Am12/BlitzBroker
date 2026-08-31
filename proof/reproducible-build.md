# Reproducible Build Proof

Generated: 2026-08-31
Toolchain: rustc 1.97.1 (8bab26f4f 2026-07-14) (pinned in rust-toolchain.toml)

## Method

Two builds from different directory paths produced byte-identical binaries.

Reproducibility achieved by:
1. `rust-toolchain.toml` — pins exact rustc version (1.97.1)
2. `.cargo/config.toml` — `--remap-path-prefix` strips checkout path from debug info
3. `[profile.release]` — `codegen-units=1` (deterministic single CGU), `debug=false`

## SHA-256

Build 1 (original path `C:\GitHub Desktop\BlitzBoard`):
`CC80623A8A28EA49F0055BCC12B46D9DC8EDF342549098507996F8C4FC45E799`

Build 2 (different path `C:\Users\kshir\AppData\Local\Temp\blitzbroker-repro-test`):
`CC80623A8A28EA49F0055BCC12B46D9DC8EDF342549098507996F8C4FC45E799`

Hashes match: YES

## Reproduce yourself

`
cargo clean
cargo build --release
Get-FileHash target\release\blitzbroker.exe -Algorithm SHA256
`

Expected SHA-256: `CC80623A8A28EA49F0055BCC12B46D9DC8EDF342549098507996F8C4FC45E799`
