# Changelog

All notable public changes to this project will be documented here.

The project follows Semantic Versioning.

## 0.1.0-alpha.1 - 2026-10-10

First public alpha release of `justybase-db2`, the JustyBase.Db2 Rust driver.

### Added

- Native Tokio TCP/TLS transport with a direct DRDA/DDM protocol implementation (no IBM CLI/ODBC runtime dependency).
- Direct and prepared SQL execution, transactions, and client-info registers.
- Stored procedures with OUT/INOUT parameters and dynamic result sets.
- TCP and TLS 1.2/1.3 connections with certificate validation; password authentication (SECMEC 3) and encrypted user/password exchange (SECMEC 9).
- Connection pooling with bounded admission, waiter, idle, and lifetime controls.
- Bounded row streaming (`Db2RowReader`), progressive LOB reads, materialization limits, spool quotas, and bounded row import with partial-progress reporting.
- BLOB / CLOB / DBCLOB / XML materialized and streamed handling depending on representation.
- Structured SQLCODE/SQLSTATE errors and fail-closed session cleanup when protocol synchronization is uncertain.
- Query cancellation via the DRDA SQL interrupt path.
- Deterministic offline DRDA replay tests plus live Db2 LUW qualification tooling.
- Cross-platform CI smoke on Linux, Windows, and macOS; live Db2 LUW 12.1.5 qualification on Linux x64.

### Notes

- Alpha software: APIs and protocol coverage may change before a stable release. Validate against your own Db2 environment and workload before production use.
- Kerberos / GSSAPI support is experimental (caller-supplied token path); see `docs/KERBEROS-STATUS.md`.
- Independent project; not maintained, endorsed, sponsored, or certified by IBM. IBM and Db2 are trademarks of International Business Machines Corporation.
