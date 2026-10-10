# Contributing

Thanks for helping improve JustyBase.Db2 for Rust.

## Development principles

Changes should preserve these properties unless the change explicitly revises the contract:

- fail closed when protocol/session state is uncertain,
- never automatically replay mutating SQL or transaction control after uncertain transport outcomes,
- keep pool admission, materialization, and spool usage bounded,
- preserve structured SQLCODE/SQLSTATE/error classification,
- avoid exposing credentials in logs/errors,
- prefer incremental parsing/streaming over large temporary allocations,
- keep public API changes intentional and reviewable.

## Toolchain

Use the Rust version pinned in `rust-toolchain.toml` and CI.

## Before submitting a change

Generate ephemeral TLS fixtures, then run the same offline checks as CI:

```bash
./scripts/generate-test-tls-fixtures.sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked
cargo test --locked --offline
cargo build --examples --locked --offline
cargo doc --no-deps --offline --quiet
node tools/api/generate-rust-surface.cjs --check
node --test tools/api/generate-rust-surface.test.cjs
python3 tools/sbom/generate_rust_sbom.py
python3 tools/sbom/assemble_rust_notices.py
python3 tools/sbom/check_package_contents.py
cargo package --locked --offline
```

The committed repository contains no TLS private keys. `scripts/generate-test-tls-fixtures.sh` creates short-lived test certificates under the ignored `tests/fixtures/tls/` directory.

## Live Db2 tests

```bash
./scripts/test-db2-integration.sh
./scripts/test-db2-integration.sh --full
```

Live-only coverage should not replace deterministic offline coverage where protocol behavior can be represented faithfully.

## Public API changes

Read `API-COMPATIBILITY.md`. Do not regenerate the API snapshot merely to make CI green; first confirm that the public contract is intentionally changing.

## Performance

For hot paths, avoid unnecessary copies and intermediate allocations, preserve bounded buffering, and benchmark meaningful changes without trading correctness for micro-optimizations. See `BENCHMARK-BASELINE.md`.

## Secrets and test data

Never commit real Db2 credentials, API tokens, private TLS keys, production database dumps, or private qualification environment files.
