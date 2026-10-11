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

## Public cross-driver parity

Selected Db2 value, metadata, DML, transaction and error semantics are independently cross-checked against IBM's .NET and JCC providers and against canonical values produced directly by Db2. The public suite lives in `tests/parity/` and `tools/parity/`; it does not use the private `db2-conformance` repository or add provider dependencies to the Rust crate.

The manual **Db2 Cross-Driver Parity** workflow runs against one isolated Db2 LUW 12.1.5 container and uploads a `db2-parity-report` artifact. Provider workers are pinned to `Net.IBM.Data.Db2-lnx` 10.0.0.300 (Linux, .NET 10; the Windows/macOS provider packages are `Net.IBM.Data.Db2`/`-osx` with the same version) and `com.ibm.db2:jcc` 12.1.5.0. The IBM binaries are downloaded from NuGet and Maven Central at run time and are not committed. To run locally, install Docker, Rust 1.98.1, .NET 10, Java 17 and Maven, then run `./scripts/test-db2-parity.sh`; use `./scripts/test-db2-integration.sh --parity-full` for the full manifest. The regular PR CI remains the Rust/live-smoke gate and does not download the IBM provider packages.

| Area | Public parity coverage |
| --- | --- |
| Numeric and NULL values | Integer boundaries, REAL/DOUBLE, DECIMAL precision and scale, DECFLOAT, BOOLEAN |
| Text and LOB | CHAR/VARCHAR padding, Unicode, GRAPHIC/VARGRAPHIC, small CLOB/DBCLOB/BLOB values, empty versus NULL |
| Temporal and binary | DATE/TIME/TIMESTAMP(0/6/12), BINARY/VARBINARY/BLOB, hex and length expressions |
| Server canonicalization | Db2 `HEX`, `LENGTH`, `VARCHAR`, `VARCHAR_FORMAT`, and `XMLSERIALIZE` expressions |
| Prepared/DML | Prepared typed insert, insert-many, update/delete row counts and final table state |
| Transactions and errors | Independent observer checks uncommitted/rolled-back/committed visibility; SQLCODE and SQLSTATE for syntax and missing-table errors |
| Independent stacks | Rust DRDA, IBM .NET provider, IBM JCC; shared SQL cases and canonical JSON Lines |

The current manifest has 55 reported cases: 52 deterministic read/canonicalization/error cases plus prepared DML/row-count/final-state and transaction-visibility checks (smoke tier: 27; full tier: 55). Error cases compare Db2 codes and states, not provider-formatted messages. This is a selected semantic cross-check, not exhaustive provider compatibility or IBM certification/support. Provider-specific CLR/Java types are not compatibility claims. Detailed parity reports record commit, server/client versions, pass counts and failing case IDs without credentials.

This initial suite does not yet cover 64 KiB/1 MiB LOB transfer in both directions, stored procedure result/warning parity, or metadata nullability parity. The transaction case checks observer visibility within each provider's own transaction flow; it does not keep one provider's transaction open while a different provider reads it. Treat those areas as unqualified until added and run successfully.

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
