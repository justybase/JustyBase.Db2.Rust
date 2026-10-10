# JustyBase.Db2 for Rust

[![CI](https://github.com/justybase/JustyBase.Db2.Rust/actions/workflows/ci.yml/badge.svg)](https://github.com/justybase/JustyBase.Db2.Rust/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/Rust-1.98.1+-000000?logo=rust)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](CHANGELOG.md)

> **Alpha software.** APIs and protocol coverage may change before a stable release. Validate the driver against your own Db2 environment and workload before production use.

Native asynchronous Rust driver for **IBM Db2 LUW**, speaking DRDA/DDM directly over TCP/TLS without IBM CLI, ODBC, or other Db2 client runtime dependencies.

- crates.io package: **`justybase-db2`**
- Rust crate identifier: **`justybase_db2`**
- repository: **`justybase/JustyBase.Db2.Rust`**

> **Independent project:** this project is not maintained, endorsed, sponsored, or certified by IBM. IBM and Db2 are trademarks of International Business Machines Corporation.

## Highlights

- Native Tokio TCP/TLS transport and direct DRDA/DDM protocol implementation.
- Direct and prepared SQL, transactions, stored procedures, pooling, cancellation, and client-info registers.
- Bounded row streaming, progressive LOB reads, materialization limits, spool quotas, and bounded row import.
- Structured SQLCODE/SQLSTATE errors and fail-closed session cleanup when protocol synchronization is uncertain.
- Deterministic offline DRDA replay tests plus live Db2 LUW qualification.
- Cross-platform managed-client CI coverage on Linux, Windows, and macOS; live Db2 qualification remains Linux x64.

## Feature matrix

| Feature | Status | Notes |
| --- | :---: | --- |
| TCP connections | ✅ | Native Tokio transport |
| TLS 1.2 / 1.3 | ✅ | Certificate validation enabled |
| Password authentication | ✅ | SECMEC 3 verified |
| SECMEC 9 | ✅ | Encrypted user/password exchange |
| Kerberos / GSSAPI | 🧪 | Caller-supplied token path; see `docs/KERBEROS-STATUS.md` |
| Direct SQL / prepared SQL | ✅ | Query and execute APIs |
| Transactions | ✅ | Commit / rollback and isolation support |
| Connection pool | ✅ | Bounded admission and lifecycle controls |
| Row streaming | ✅ | Pull-based `Db2RowReader` |
| BLOB / CLOB / DBCLOB / XML | ✅ | Materialized and streamed paths depending on representation |
| Progressive LOB streaming | ✅ | Bounded locator reads |
| Stored procedures | ✅ | OUT/INOUT and dynamic result sets |
| Batch DML / row import | ✅ | Bounded execution with partial-progress reporting |
| Query cancellation | ✅ | DRDA SQL interrupt path |
| Trusted-context switching | 🧪 | Requires matching server configuration |
| IBM CLI dependency | ❌ | Not required at runtime |

`✅` means implemented and covered by automated tests. `🧪` means environment- or server-configuration-dependent and requires explicit validation.

## Requirements

- Rust **1.98.1** or newer within the currently tested toolchain line.
- Tokio runtime.
- IBM Db2 LUW reachable over TCP or TLS.

The maintained live qualification baseline is Db2 LUW 12.1.5 on Linux x64. Windows and macOS are continuously exercised by protocol/API smoke tests. See [Qualification and compatibility](docs/QUALIFICATION.md).

## Installation

No crates.io version has been published yet. Until the first release, use a local path dependency while developing from this repository:

```toml
[dependencies]
justybase-db2 = { path = "../JustyBase.Db2.Rust" }
```

After publication, the dependency will use the crates.io version instead. In Rust source code import the crate as `justybase_db2`.

## Quick start

```rust
use justybase_db2::Db2Session;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Db2Session::connect_dsn(
        "db2://user:password@localhost:50000/TESTDB",
    )
    .await?;

    let result = db.query("VALUES 1").await?;
    println!("{:?}", result.rows);

    db.close().await?;
    Ok(())
}
```

Use environment variables or a secret store for credentials. Do not embed production passwords in source code.

## TLS

Use `db2s://` and configure a trusted CA/server certificate. IBM CLI-style key/value connection strings are supported for the documented subset, including `Security=SSL` and `SSLServerCertificate=...`. Unsupported security modes fail validation rather than silently falling back to plaintext.

## Prepared statements

```rust
use justybase_db2::Db2Value;

let statement = db.prepare("VALUES CAST(? AS INTEGER)").await?;
let result = db
    .query_prepared(&statement, &[Db2Value::Integer(42)])
    .await?;
```

## Streaming and large objects

```rust
let mut rows = db.query_stream(
    "SELECT ID, NAME FROM APP.CUSTOMERS ORDER BY ID"
).await?;

while let Some(row) = rows.next().await? {
    println!("{:?}", row);
}
rows.close().await?;
```

Materialized LOB values are deliberately bounded. The current alpha limits a single materialized LOB to **16 MiB** and a materialized query result to **32 MiB**. Supported locator values can be consumed incrementally through the row-stream path.

## Pooling and cancellation

The pool has bounded admission, waiter, idle, and lifetime controls. Db2 cancellation uses a separate DRDA SQL interrupt exchange; an interrupt acknowledgment does not by itself prove that the original command has stopped. See [Advanced driver guide](docs/DRIVER-GUIDE.md) for lifecycle and cleanup guarantees.

## Testing

Full offline tests use ephemeral TLS certificates generated locally; no private test keys are committed:

```bash
./scripts/generate-test-tls-fixtures.sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked
cargo test --locked --offline
cargo build --examples --locked --offline
cargo doc --no-deps --offline --quiet
node tools/api/generate-rust-surface.cjs --check
python3 tools/sbom/check_package_contents.py
```

Live Db2 tests are explicit:

```bash
./scripts/test-db2-integration.sh        # release smoke suite
./scripts/test-db2-integration.sh --full # full live qualification suite
```

The integration script starts an isolated pinned Db2 Community container and removes its resources afterwards. Running the IBM image means accepting its applicable license terms.

## Public API policy

The alpha API is mechanically guarded in CI. See [API compatibility policy](API-COMPATIBILITY.md). Low-level protocol modules that are public for tooling/tests are not the preferred application API and may be reduced before 1.0.

## Documentation

- [Advanced driver guide](docs/DRIVER-GUIDE.md)
- [Qualification and compatibility](docs/QUALIFICATION.md)
- [Kerberos / GSSAPI status](docs/KERBEROS-STATUS.md)
- [API compatibility policy](API-COMPATIBILITY.md)
- [Benchmark methodology](BENCHMARK-BASELINE.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Changelog](CHANGELOG.md)

## License and provenance

The project is licensed under the [MIT License](LICENSE).

The implementation uses the MIT-licensed [`go-db2`](https://github.com/go-db2/go-db2) project as a primary DRDA protocol and implementation reference. Required upstream notices for derived or adapted portions are retained in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

This repository does not bundle IBM CLI libraries or IBM driver binaries. Application and server licensing requirements remain the responsibility of the user.
