# Reproducible Build Proof

Generated: 2026-08-31
Toolchain: rustc 1.97.1 (8bab26f4f 2026-07-14) (pinned in rust-toolchain.toml)

## Method

Two consecutive **clean** builds (`cargo clean && cargo build --release` each time)
and two builds from **different directory paths** all produced the same byte-identical binary.

Reproducibility achieved by four coordinated mechanisms:

1. `rust-toolchain.toml` — pins `rustc 1.97.1`; rustup installs it automatically.
2. `.cargo/config.toml` rustflags:
   - `--remap-path-prefix =blitzbroker/` — strips the absolute checkout path from
     all source-path references embedded in the binary.
   - `-C link-arg=/Brepro` — instructs the MSVC linker to replace the live
     `TimeDateStamp` field in the PE COFF header (offset 0xF0) with a hash derived
     from the binary's own contents. Without this flag the linker stamps the current
     wall-clock time into the header on every build, making every binary unique.
3. `[profile.release]` in `Cargo.toml`:
   - `codegen-units = 1` — single deterministic CGU, no parallel-ordering
     non-determinism in symbol layout.
   - `debug = false` — removes the debug-info section (belt-and-suspenders path
     stripping alongside --remap-path-prefix).

## SHA-256

All four builds (2x clean-build, 2x different-path):
`4D62CFB48F1B3377A7AC24C302E5FCF5D63604BAC89DBB5D0C7FE78822FF0278`

## Reproduce yourself

```
cargo clean
cargo build --release
# On Windows (PowerShell):
(Get-FileHash target\release\blitzbroker.exe -Algorithm SHA256).Hash
# On Linux/macOS:
sha256sum target/release/blitzbroker
```

Expected: `4D62CFB48F1B3377A7AC24C302E5FCF5D63604BAC89DBB5D0C7FE78822FF0278`

Note: /Brepro is an MSVC linker flag. On Linux/macOS (GNU ld / lld), the PE
TimeDateStamp issue does not apply, but the --remap-path-prefix flag still ensures
path-independent output on those platforms.
