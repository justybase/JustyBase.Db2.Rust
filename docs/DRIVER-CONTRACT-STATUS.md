# JDC v1 status — Rust

Contract: [`docs/DRIVER-CONTRACT.md`](DRIVER-CONTRACT.md) (JDC v1).
Last reviewed: 2026-11-09.

This file is the repo-specific conformance record. It is not part of the
normative text. `compliant` means every `MUST` in the group is met; `partial`
means some are met and the rest are listed; `gap` means the group is not met.
`gap` and `partial` entries are implementation work for the next conformance
pass; this file records them, it does not waive the requirement.

## Summary

| Status | Requirements |
| --- | --- |
| compliant | ERR-01-09, OUT-01, OUT-06-09, DSN-01-06, DSN-08-10, LIM-01-08, POOL-01-07, SES-01-10, VAL-01-10, CAN-01-09, BAT-01-09 |
| partial | OUT-02-05, DSN-07 |
| gap | — |

## Details

| Requirements | Status | Notes |
| --- | --- | --- |
| ERR-01 | compliant | `Db2Error` exposes kind/outcome/message/SQLCODE/SQLSTATE; `Db2BatchError` carries a cause and wrapper `kind()`/`outcome()`. Connect and TLS failures are classified. |
| ERR-02 | compliant | `Db2ErrorKind` covers all canonical categories, including `OperationCancelled`, `Timeout` and `BatchExecution`. |
| ERR-03–04 | compliant | SQL/auth/authz/conversion/transport/protocol/timeout/cancellation map to stable kinds; confirmed `-952` maps to `OperationCancelled` in SQLCARD and SQLERRRM paths. |
| ERR-05–08 | compliant | SQLCODE/SQLSTATE, message format, redaction and batch cause/progress are preserved. |
| ERR-09 | compliant | Enum spellings are idiomatic; serialized consumers can map them one-to-one to the canonical identifiers. |
| OUT-01 | compliant | `Db2OperationOutcome` is `NotStarted`/`Confirmed`/`Unknown`. |
| OUT-02–05 | partial | Deadline setters and connect failures set the outcome from `exchange_pending`/the connect phase; other `Db2Error::new(Transport, …)` sites still default to `unknown` even before a write. |
| OUT-06–07 | compliant | No automatic replay; uncertain sessions are discarded, and `is_closed()` includes the poisoned coordinator. |
| OUT-08 | compliant | `Db2BatchError::outcome()` returns `confirmed`, `not-started` or `unknown` from the cause and failed-set position. |
| OUT-09 | compliant | A confirmed `-952` is `OperationCancelled`/`Confirmed`; unconfirmed settlement is `Unknown`. |
| DSN-01 | compliant | Case-insensitive key/value and URL parsing; last value wins. |
| DSN-03–06 | compliant | `currentschema`, security mechanisms, TLS and authentication shorthand are implemented; the schema is validated before I/O, applied with a quoted identifier after connect and restored by the pool reset path. An explicit empty schema is rejected. |
| DSN-02 | compliant | Every JDC canonical key and alias is accepted, including `kerberos_spn`, the principal aliases and the Kerberos file keys. |
| DSN-07 | partial | Supports `max_dss_size`, `max_ddm_object_size`, `max_reply_chain_bytes`, `max_reply_chain_packets`, `max_materialized_lob_size`; missing `max_streamed_lob_bytes` and `lob_chunk_bytes` (the SHOULD for LOB/streamed limits is only partly met). |
| DSN-08 | compliant | Unknown `krb5`/`kerberos`/`gssapi` keys fail closed with a configuration error instead of being stored in `params`. |
| DSN-09–10 | compliant | No password in diagnostics; required identity values fail before I/O. |
| LIM-01 | compliant | Every configurable limit rejects zero, negative, non-numeric and above-ceiling values with a configuration error; fixed limits document their JDC defaults. |
| LIM-02 | compliant | Defaults are 64 MiB DSS, 64 MiB DDM, 256 MiB reply chain, 4096 packets, 16 MiB LOB, 32 MiB result, 1 GiB streamed and 64 KiB chunks. |
| LIM-03 | compliant | Hard ceilings are enforced for all five configurable limits (1 GiB / 1 GiB / 4 GiB / 65535 / 1 GiB). |
| LIM-04-06 | compliant | Materialized limits fail closed and DSS/DDM/reply-chain overflows are resource-limit errors. |
| LIM-07 | compliant | `Db2BatchOptions` defaults to 256 rows / 8 MiB and the byte limit is soft; import staging matches. |
| LIM-08 | compliant | The shared spool budget is bounded and released on success/error/cancellation. |
| POOL-01-06 | compliant | Defaults, admission errors, reset/discard and drain/shutdown match; `max_waiters = 0` is admitted. |
| POOL-07 | compliant | `Db2PoolStats`/`stats()` expose total, idle, active, waiters, discarded sessions and acquire timeouts; individual getters remain available. |
| SES-01–03 | compliant | Connect/close, prepare/execute and commit/rollback match. |
| SES-04 | compliant | `change_database` validates the target before any I/O, requires an open session, refuses an active transaction or pooled lease even for the current name, reconnects with a fresh handshake and leaves the session closed if the reconnect fails. |
| SES-05–10 | compliant | Current schema setter/getter, isolation setter, per-session transactions, ordered batch and cancellation support are present. |
| VAL-01–10 | compliant | DECIMAL/DECFLOAT and temporal values use exact strings; NULL, binary, CP500, metadata and progressive LOB rules are met. |
| CAN-01–02 | compliant | Two-step interrupt and original-operation settlement are implemented. |
| CAN-03 | compliant | A confirmed `-952` is `OperationCancelled` with `Confirmed`; SQLCODE/SQLSTATE/outcome are preserved. |
| CAN-04–07 | compliant | Race handling, unconfirmed discard, idle cancellation and idempotent cancel are covered. |
| CAN-08 | compliant | Connect, command and settlement deadlines produce `Timeout` with `NotStarted`/`Unknown` from the exchange phase. |
| CAN-09 | compliant | Cancellation is safe while the operation lock is held. |
| BAT-01 | compliant | Ordered execution, stop-on-first-failure and completed/affected counts. |
| BAT-02 | compliant | `Db2BatchOptions` defaults to 256 rows / 8 MiB, validates `max_rows` in 1..32767 and treats the byte limit as soft. |
| BAT-03-04 | compliant | Batch cause/progress, import policies and atomic-only table lock match. |
| BAT-05 | compliant | Import defaults are 256 rows, 8 MiB staged bytes, progress every 10,000 rows, atomic policy and table lock off. |
| BAT-06–09 | compliant | 1 GiB streamed budget, 64 KiB chunks, progressive cancel, fail-closed materialization and the large-data path are implemented. |
