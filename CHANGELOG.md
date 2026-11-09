# Changelog

All notable public changes to this project will be documented here.

The project follows Semantic Versioning.

## Unreleased

### Changed

- Updated direct dependencies to the latest stable releases, including tokio
  1.54.1, tokio-rustls 0.26.6, rustls-native-certs 0.8.5, rustls-pki-types
  1.15.1, des 0.9.0, crypto-bigint 0.7.11 and criterion 0.8.2. The DES helpers
  now use the cipher 0.5 block API.
- Added the self-contained JDC v1 contract (`docs/DRIVER-CONTRACT.md`) and the
  repo-specific conformance record (`docs/DRIVER-CONTRACT-STATUS.md`). No API or
  behaviour changes.
- Added `Db2ErrorKind::OperationCancelled`, `Db2ErrorKind::Timeout` and
  `Db2ErrorKind::BatchExecution`; `Db2Error::sql`/`server_diagnostic` map `-952`
  to a confirmed cancellation. `Db2Error::with_outcome`, `Db2Error::timeout`
  and `Db2Error::cancelled` make the operation phase explicit, and
  `Db2BatchError::kind`/`outcome` expose the wrapper classification. Connect,
  handshake and TLS deadline failures are now `Timeout`/`Transport`. API snapshot
  regenerated for the additive enum methods.
- Added `currentschema`/`current_schema` DSN support and `Config::current_schema`/
  `krb5_principal`, plus all JDC Kerberos aliases (`kerberos_spn`,
  `kerberos_realm`, the principal spellings and the file keys). Unknown
  `krb5`/`kerberos`/`gssapi` keys fail closed, and an explicit empty schema is
  rejected. `Config` is a public struct without `#[non_exhaustive]`, so adding
  the public `krb5_principal` field is an intentional pre-1.0 break for
  struct-literal construction; `Config::default()` and `parse_dsn` are
  unaffected. `Db2Session::change_database` reconnects with a fresh handshake,
  validates an open session, refuses a transaction or pooled lease before I/O,
  and leaves the session closed on failure; a same-name switch is a no-op only
  after those checks. `set_current_schema` now trims and validates its input.
  API snapshot regenerated for the additive members.
- `execute_batch_with_params_and_options_and_limits` validates the batch bounds
  before the empty-parameter-set shortcut, so invalid limits fail even when
  there is nothing to send.
- Aligned resource limits with JDC: limit DSN values now reject zero, negative,
  non-numeric and above-ceiling values with a configuration error; invalid
  `block_size` fails too. `Db2BatchOptions` defaults to 256 rows / 8 MiB, the
  import default reports progress every 10,000 rows, and `Db2PoolStats`/`stats()`
  expose total/idle/active/waiters/discarded/acquire-timeout counters. API
  snapshot regenerated for the additive members.

## 0.1.0-alpha.3 - 2026-10-21

### Added

- DRDA `USRENCPWD` (SECMEC 7) authentication: `security_mechanism=7` (`usrencpwd`, `encrypted_password_security`, `secmec7`) sends a clear user ID and the DES-CBC-encrypted password in `SECCHK/SECTKN`, with the client DH key in `ACCSEC` (`ENCALG=1`) and the IV taken from the CP500 user ID. AES (`ENCALG=2`) and other algorithms fail closed before credentials are sent. New experimental helper `security::encrypt_usrencpwd` (recorded in the API snapshot). Opt-in live tests `tests/secmec7_live.rs` (`DB2_SECMEC7_TEST_DSN`).

### Changed

- A connection without a configured `security_mechanism` now follows a server that refuses the plaintext default: when `ACCSECRD` lists `9` or `7` (Db2 `AUTHENTICATION=SERVER_ENCRYPT`), the driver sends a second `ACCSEC` on the same connection with `9`, else `7`, before any credentials are sent. An explicitly configured mechanism, including `3`, is never changed, and a rejection of the retried mechanism ends the handshake.
- `ACCSECRD`'s SECMEC and ENCALG parameters are decoded as lists. A rejected mechanism fails with `Db2ErrorKind::Unsupported` (previously `Other`) naming every mechanism the server accepts plus `SECCHKCD` (`DB2 rejected security mechanism 3 (SECCHKCD=1); the server accepts 7, 9, 10`). A non-zero `SECCHKCD` for the requested mechanism fails with `AuthenticationFailed` before `SECCHK`.
- The `switch_user_with_password` SECCHK fallback checks the mechanism actually negotiated, so it stays refused after an automatic switch.
- Verified live against Db2 LUW 11.5.8.0 with `AUTHENTICATION=SERVER_ENCRYPT`.

## 0.1.0-alpha.2 - 2026-10-15

Second alpha release of `justybase-db2`.

### Added

- `examples/usage.rs`: a usage example covering connection, queries, a prepared statement, a transaction, and outcome-based error handling. Run it with `DB2_DSN` set.

### Changed

- `security::generate_private_key` and `security::calculate_session_key` return `Zeroizing<[u8; 32]>` (the `zeroize` crate). The SECMEC 9 Diffie-Hellman private key and shared secret are overwritten when the value is dropped. The value dereferences to `[u8; 32]`; this is an experimental helper, so the signature change is recorded in the API snapshot.
- The SECMEC 9 private exponent limbs are overwritten after each modular exponentiation.
- Corrected the `0.1.0-alpha.1` release date; installation examples and the API policy no longer name a single alpha version.

## 0.1.0-alpha.1 - 2026-10-15

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

- When `ssl_root_ca_path` / `SSLServerCertificate` is set, that CA file replaces the system trust store instead of being added to it. A CA file without any certificate is rejected.
- Protocol/codec modules (`network::{codepoint,ddm,dss,stream}`) and the `replay` harness are `#[doc(hidden)]`: they are no longer in rustdoc or the API snapshot and carry no compatibility promise.
- `Db2ImportPolicy`, `Db2OperationOutcome`, `Db2IsolationLevel`, `Db2LobEncoding` and `Db2ParameterDirection` are `#[non_exhaustive]`.
- `rustls` is 0.23.46 (RUSTSEC-2026-0346 through 0349).
- SECMEC 9 Diffie-Hellman arithmetic uses `crypto-bigint` constant-time modular exponentiation instead of a hand-written bignum. Public signatures and the DRDA wire format are unchanged; verified against the Derby/OpenSSL vectors and a live SERVER_ENCRYPT Db2 server.
- Alpha software: APIs and protocol coverage may change before a stable release. Validate against your own Db2 environment and workload before production use.
- Kerberos / GSSAPI support is experimental (caller-supplied token path); see `docs/KERBEROS-STATUS.md`.
- Independent project; not maintained, endorsed, sponsored, or certified by IBM. IBM and Db2 are trademarks of International Business Machines Corporation.
