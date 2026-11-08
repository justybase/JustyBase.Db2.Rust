# JDC v1 status — Rust

Contract: [`docs/DRIVER-CONTRACT.md`](DRIVER-CONTRACT.md) (JDC v1).
Last reviewed: 2026-11-08.

This file is the repo-specific conformance record. It is not part of the
normative text. `compliant` means every `MUST` in the group is met; `partial`
means some are met and the rest are listed; `gap` means the group is not met.
`gap` and `partial` entries are implementation work for the next conformance
pass; this file records them, it does not waive the requirement.

## Summary

| Status | Requirements |
| --- | --- |
| compliant | ERR-01–09, OUT-01, OUT-06–09, DSN-01, DSN-03–06, DSN-09–10, LIM-02, LIM-04–06, LIM-08, POOL-01–06, SES-01–03, SES-05–10, VAL-01–10, CAN-01–09, BAT-01, BAT-03–04, BAT-06–09 |
| partial | OUT-02–05, DSN-02, DSN-07 |
| gap | DSN-08, LIM-01, LIM-03, LIM-07, POOL-07, SES-04, BAT-02, BAT-05 |

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
| DSN-03–06 | compliant | `currentschema`, security mechanisms, TLS and authentication shorthand are implemented. |
| DSN-02 | partial | Core aliases are present except `kerberos_spn`, `krb5_principal`/`kerberos_principal`/`client_principal`. |
| DSN-07 | partial | Supports `max_dss_size`, `max_ddm_object_size`, `max_reply_chain_bytes`, `max_reply_chain_packets`, `max_materialized_lob_size`; missing `max_streamed_lob_bytes` and `lob_chunk_bytes` (the SHOULD for LOB/streamed limits is only partly met). |
| DSN-08 | gap | Unknown `krb5`/`kerberos`/`gssapi` keys are stored in `params` instead of failing closed. |
| DSN-09–10 | compliant | No password in diagnostics; required identity values fail before I/O. |
| LIM-01 | gap | `set_positive_limit` silently ignores zero, negative and non-numeric values; configurable limits have no hard ceilings. |
| LIM-02 | compliant | Defaults are 64 MiB DSS, 64 MiB DDM, 256 MiB reply chain, 4096 packets, 16 MiB LOB, 32 MiB result, 1 GiB streamed and 64 KiB chunks. |
| LIM-03 | gap | The five configurable limits accept any positive value; there is no documented/enforced hard ceiling. |
| LIM-04–06 | compliant | Materialized limits fail closed and reply-chain overflows are resource-limit errors. |
| LIM-07 | gap | `Db2BatchOptions` defaults to 4 MiB, while JDC requires 8 MiB; import staging matches. |
| LIM-08 | compliant | The shared spool budget is bounded and released on success/error/cancellation. |
| POOL-01–06 | compliant | Defaults, admission errors, reset/discard and drain/shutdown match. |
| POOL-07 | gap | Public counters cover only session/idle/waiting; discarded sessions and acquire timeouts are not exposed. |
| SES-01–03 | compliant | Connect/close, prepare/execute and commit/rollback match. |
| SES-04 | gap | No `change_database`/reconnect verb. |
| SES-05–10 | compliant | Current schema setter/getter, isolation setter, per-session transactions, ordered batch and cancellation support are present. |
| VAL-01–10 | compliant | DECIMAL/DECFLOAT and temporal values use exact strings; NULL, binary, CP500, metadata and progressive LOB rules are met. |
| CAN-01–02 | compliant | Two-step interrupt and original-operation settlement are implemented. |
| CAN-03 | compliant | A confirmed `-952` is `OperationCancelled` with `Confirmed`; SQLCODE/SQLSTATE/outcome are preserved. |
| CAN-04–07 | compliant | Race handling, unconfirmed discard, idle cancellation and idempotent cancel are covered. |
| CAN-08 | compliant | Connect, command and settlement deadlines produce `Timeout` with `NotStarted`/`Unknown` from the exchange phase. |
| CAN-09 | compliant | Cancellation is safe while the operation lock is held. |
| BAT-01 | compliant | Ordered execution, stop-on-first-failure and completed/affected counts. |
| BAT-02 | gap | `Db2BatchOptions` exists, but the default byte bound is 4 MiB instead of 8 MiB. |
| BAT-03–04 | compliant | Batch cause/progress, import policies and atomic-only table lock match. |
| BAT-05 | gap | Import defaults match except `notify_after_rows` is `None` (no 10,000-row periodic report). |
| BAT-06–09 | compliant | 1 GiB streamed budget, 64 KiB chunks, progressive cancel, fail-closed materialization and the large-data path are implemented. |
