# API compatibility policy — JustyBase.Db2 for Rust

The project mechanically tracks its consumer-visible Rust API so accidental public-surface changes fail CI.

Two artifacts work together:

- `api/rust-surface.json` — canonical public signatures parsed from pinned-toolchain offline rustdoc output.
- `examples/api_guard.rs` — a compile-time guard that references root re-exports so removals/renames fail even when the canonical item still exists elsewhere.

## Stability tiers

| Tier | Surface | Alpha policy |
| --- | --- | --- |
| Supported alpha | `Config`/`parse_dsn`, `Db2Session`, `Db2Pool`, structured errors, row/value readers, import API | Changes must be deliberate, documented, and reflected in the API snapshot. |
| Experimental | Security/auth helpers, interrupt edge semantics, selected import/session controls | May change between alpha releases, but changes must remain explicit and documented. |
| Low-level unstable | Protocol/codec modules (`network::{codepoint,ddm,dss,stream}`) and the `replay` test harness, marked `#[doc(hidden)]` | Not in rustdoc and not in the API snapshot; no compatibility promise. May be reduced/internalized before stable release. |

Before 1.0, Semantic Versioning permits breaking changes through minor-version changes. The `0.1.0` alpha releases are intentionally not a stable 1.0 contract.

Public enums that may gain variants (`Db2ImportPolicy`, `Db2OperationOutcome`, `Db2IsolationLevel`, `Db2LobEncoding`, `Db2ParameterDirection`) are `#[non_exhaustive]`: match them with a wildcard arm outside the crate. Adding a variant to these is not a breaking change.

## Commands

```bash
cargo doc --no-deps --offline
node tools/api/generate-rust-surface.cjs
node tools/api/generate-rust-surface.cjs --check
cargo build --examples --locked --offline
```

Use the pinned repository Rust toolchain when generating/checking the snapshot so rustdoc-format changes do not become unrelated API noise.

## Change process

1. Decide whether the API change is intentional.
2. Update implementation/tests.
3. Update README, changelog, and affected guides.
4. Regenerate the snapshot and adjust `examples/api_guard.rs` in the same reviewed change.
5. Provide migration/deprecation guidance for supported-alpha consumers where practical.

Do not blindly regenerate the snapshot only to make CI pass.
