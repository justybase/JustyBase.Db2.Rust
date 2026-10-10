# Qualification and compatibility

This document describes the maintained release gates for the current alpha.

## Maintained baseline

- Rust 1.98.1.
- Managed client build/protocol smoke on Linux, Windows, and macOS.
- Live Db2 LUW 12.1.5 qualification on Linux x64 over TCP and TLS.

## Automated CI gate

Every push/PR runs:

- formatting and clippy checks,
- the complete offline unit/protocol/replay suite on Ubuntu,
- compile/protocol smoke on Ubuntu, Windows, and macOS,
- public API snapshot verification,
- SBOM, third-party notices, and crate-content validation,
- `cargo package` verification,
- an isolated Db2 LUW live smoke suite on Ubuntu,
- a final `CI gate` job that requires all lanes above.

The cross-platform jobs validate the Rust client path; only Linux x64 is currently live-qualified against Db2.

## Full release qualification

Before publishing a release candidate, the maintainer-run full qualification additionally covers:

- complete deterministic replay suites and block-size matrix,
- live TCP and TLS protocol behavior,
- early-terminate/drain behavior,
- cancellation / SQL interrupt cleanup,
- large-value streaming including a 1 GiB BLOB checksum workload,
- bounded process memory and spool usage,
- a measured soak after warm-up,
- cross-driver semantic checks where applicable,
- exact source revision/toolchain/environment recording.

A source change after successful qualification invalidates that qualification for release purposes until the updated revision is tested again.

## Current resource budgets

The maintained qualification harness uses up to 10 concurrent sessions, a 512 MiB process RSS budget, a 1 GiB large streamed LOB workload, a process-wide 10 GiB spool budget, and a 15-minute measured soak after warm-up. These are qualification gates, not universal application recommendations.

## Public implementation limits

- single materialized LOB: 16 MiB,
- materialized query result: 32 MiB,
- progressive row-stream LOB values use bounded incremental reads,
- uncertain protocol cleanup causes fail-closed session disposal rather than unsafe reuse.

## Scope

Db2 for z/OS and IBM i are not part of the maintained qualification baseline. Kerberos/GSSAPI and trusted-context scenarios depend on external configuration and require explicit validation. This project is independent and is not IBM-certified or IBM-supported.
