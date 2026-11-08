<!-- normative; v1; sha256:ea7fb4d717734680bee20bbfa598241cfdde809309ab756374f9505f70badb7f; generated from the maintainer contract; do not edit -->
# JustyBase Db2 Contract (JDC) v1

Status: **normative**. This document defines the observable behaviour that every
JustyBase Db2 driver must provide. It does not prescribe internal structure,
class names, file layout or language style. Each requirement has a stable ID so
that implementation status and shared test vectors can refer to it.

A driver is conformant when every `MUST` requirement is satisfied, every `SHOULD`
deviation is documented in that driver's `docs/DRIVER-CONTRACT-STATUS.md`, and
every omission allowed by this document is listed there.

## Conventions

- **MUST** / **MUST NOT** — required; a divergence is a defect.
- **SHOULD** / **SHOULD NOT** — expected unless the language or platform makes it
  impossible; the deviation and its reason must be documented.
- **MAY** — optional.
- "Driver" means the native public API of one implementation (C#, TypeScript,
  Rust or Python). Compatibility facades inherit these rules for the operations
  they expose.
- "Server-confirmed" means Db2 returned a SQLCODE/SQLSTATE pair for the
  operation.
- Values in tables are defaults unless stated otherwise. `MiB` means 1024×1024
  bytes.

## Versioning

JDC v1 is a stable contract. Additive requirements use v1.x and keep every
existing ID. Removing or changing the meaning of a requirement requires JDC v2.
A driver may implement a newer v1.x before this document is updated, but it must
not claim conformance to a requirement it does not meet.

## 1. Error classification

- **ERR-01** Every operation failure MUST expose: a stable kind, a
  human-readable message, `sqlCode` and `sqlState` when Db2 supplied them,
  `securityCode` for a DRDA SECCHKCD failure, and an operation outcome
  (section 2). The fields MUST be readable without parsing the message.
- **ERR-02** The kind MUST map to one of these canonical categories:

  | Canonical kind | Meaning |
  | --- | --- |
  | `configuration` | Invalid DSN, option or client-side configuration. |
  | `connection-closed` | The session or pool was already closed or discarded. |
  | `database-not-found` | The requested relational database does not exist. |
  | `authentication` | Supplied credentials were rejected. |
  | `authorization` | An authenticated identity was denied an operation. |
  | `parameter-conversion` | A client value cannot be encoded for the target type. |
  | `sql-server` | Db2 returned a SQL error with SQLCODE/SQLSTATE. |
  | `server-response` | A server reply was incomplete or semantically invalid. |
  | `transport` | TCP/TLS I/O failed. |
  | `protocol` | A DRDA/DDM payload could not be parsed or was inconsistent. |
  | `resource-limit` | A configured memory, row, byte or spool bound was hit. |
  | `operation-cancelled` | The operation was confirmed cancelled. |
  | `timeout` | A connect, command or cancellation settlement deadline expired. |
  | `unsupported` | The driver does not implement the requested feature. |
  | `pool-timeout` | Acquiring a pool lease timed out. |
  | `pool-exhausted` | The pool waiter bound was reached. |
  | `batch-execution` | A batch failed after earlier parameter sets may have run. |
  | `other` | None of the above; never used for a category that fits another row. |

- **ERR-03** Classification MUST follow these rules, in order:
  1. A complete server SQLCODE/SQLSTATE pair is `sql-server`.
  2. A malformed or incomplete server reply is `server-response`.
  3. A local DRDA/DDM parse or state inconsistency is `protocol`.
  4. A socket/TLS failure is `transport`.
  5. A credential rejection during connection setup is `authentication`; an
     authorization denial is `authorization`.
  6. A client-side value encoding failure is `parameter-conversion`.
  7. A confirmed cancellation is `operation-cancelled`.
  8. A deadline is `timeout`; pool admission deadlines are `pool-timeout` or
     `pool-exhausted` as defined in section 5.
  9. A local limit is `resource-limit`; an unimplemented feature is
     `unsupported`; invalid client configuration is `configuration`.
  10. Anything else is `other`.
- **ERR-04** The same server failure MUST produce the same canonical kind in
  every driver. Language-specific extra categories MAY exist but MUST map to a
  canonical category in the driver's status document.
- **ERR-05** When Db2 supplies SQLCODE/SQLSTATE, both MUST be preserved verbatim
  (SQLSTATE upper-case, five characters when present). A SQLCODE with a missing
  SQLSTATE is still `sql-server`; a missing SQLCODE with a valid SQLSTATE is
  `sql-server`.
- **ERR-06** A `sql-server` message MUST contain `SQLCODE=<n>` and, when
  present, `SQLSTATE=<state>` so existing log parsers keep working.
- **ERR-07** Credentials, password-derived keys, tokens and private-key material
  MUST NOT appear in any error message, log line, `toString()`/`Display` output
  or configuration dump.
- **ERR-08** A batch failure MUST expose the failed parameter-set index (nullable
  when setup failed), the number of completed sets, the rows affected by
  completed sets, and the original cause. The cause's kind, SQLCODE and
  SQLSTATE MUST remain available.
- **ERR-09** A driver MAY use its own spellings (`Authentication`,
  `authentication_failed`, …). Serialized/JSON forms MUST use the canonical
  kebab-case identifier from ERR-02.

## 2. Operation outcome

- **OUT-01** The outcome is exactly one of `not-started`, `confirmed` or
  `unknown`. Serialized/JSON forms use these spellings; language enums may use
  `NotStarted` / `not_started` internally.
- **OUT-02** `not-started` means no request bytes for the operation were written
  to the server and no server-side effect is possible. This includes configuration
  rejection, pool admission failure and a limit check that runs before the first
  write.
- **OUT-03** `confirmed` means Db2 acknowledged the operation with a SQLCODE/
  SQLSTATE pair, including `-952`/`57014` for a confirmed cancellation.
- **OUT-04** `unknown` means the operation may have reached the server (request
  bytes may have been written) and no server-confirmed result is available.
- **OUT-05** The outcome MUST be derived from the operation phase, not only from
  the error kind. A transport failure before the first write is `not-started`; the
  same failure after a write is `unknown`.
- **OUT-06** A mutating statement, a batch, or transaction control MUST NOT be
  replayed automatically when the outcome is `unknown`. Automatic replay is
  permitted only when the outcome is `not-started` and the operation is
  idempotent by contract.
- **OUT-07** A pooled session whose outcome is `unknown` MUST be discarded and
  its transport closed; it MUST NOT return to the pool.
- **OUT-08** A batch wrapper reports the strongest applicable outcome: `unknown`
  if the failing parameter set may have executed, `confirmed` if Db2 confirmed
  its failure and no set remains in flight. `completedParameterSets` and
  `rowsAffected` MUST always be available.
- **OUT-09** Cancellation outcomes follow section 8: confirmed `-952` is
  `confirmed`; a cancellation whose settlement is uncertain is `unknown`; a
  cancellation that loses the race and lets the operation succeed is not an
  error.

## 3. DSN and configuration keys

- **DSN-01** Key names are case-insensitive. The last occurrence of a key wins.
  Both `key=value;key=value` strings and `db2://` URL query parameters are
  supported where the driver advertises a URL form.
- **DSN-02** The following canonical keys and aliases MUST be accepted. A driver
  MAY accept additional aliases, but the canonical key and the listed aliases
  must behave identically.

  | Canonical key | Required aliases | Meaning |
  | --- | --- | --- |
  | `host` | `hostname`, `server` (endpoint form) | Server host name. |
  | `port` | — | TCP port; default 50000, TLS default 50001 when the driver uses the Db2 default. |
  | `database` | `dbname`, `db` | Relational database name. |
  | `user` | `uid`, `username` | User name. |
  | `password` | `pwd` | Password. Never logged. |
  | `currentschema` | `current_schema` | `CURRENT SCHEMA` applied after connect. |
  | `security` | `ssl`, `use_ssl`, `usessl` | Enable TLS (`SSL`/true). |
  | `ssl_root_ca` | `ssl_ca`, `sslrootcapath`, `ssl_ca_path`, `sslservercertificate`, `ssl_server_certificate` | Trusted CA file. |
  | `ssl_client_cert_path` | `ssl_client_cert`, `sslcert`, `cert` | Client certificate file. |
  | `ssl_client_key_path` | `ssl_client_key`, `sslkey`, `key` | Client private-key file. |
  | `timeout` | `connect_timeout` | Connect/command deadline, duration syntax. |
  | `block_size` | `blocksize`, `qryblksz` | Query block size, 1024..1048576. |
  | `security_mechanism` | `secmec`, `auth_mechanism`, `auth` | DRDA security mechanism. |
  | `spn` | `service_principal`, `server_principal`, `kerberos_spn` | Kerberos service principal. |
  | `realm` | `krb5_realm`, `kerberos_realm` | Kerberos realm. |
  | `krb5_principal` | `krb5principal`, `kerberos_principal`, `kerberosprincipal`, `client_principal` | Client principal. |
  | `krb5_config` | `krb5_conf` | Kerberos configuration file. |
  | `krb5_keytab` | `keytab` | Kerberos keytab file. |
  | `krb5_ccache` | `ccache` | Kerberos credential cache. |
  | `gssapi_token_file` | `gssapi_token` | Pre-acquired GSSAPI token file. |
  | `client_applname` | `applname`, `application_name`, `appname` | Client application name. |
  | `client_wrkstnname` | `wrkstnname`, `workstation_name`, `workstation` | Client workstation name. |
  | `client_userid` | `client_user`, `end_user` | Client end-user ID. |
  | `client_acctng` | `acctng`, `accounting` | Client accounting string. |
  | `client_corr_token` | `corr_token`, `correlation_token` | Client correlation token. |

- **DSN-03** `currentschema`/`current_schema` MUST be applied at connect with a
  quoted identifier, MUST reject an empty value, values over 128 characters and
  values containing NUL, and MUST be restored by the pool reset path.
- **DSN-04** `security_mechanism` values MUST accept `0`, `3`, `7`, `9`, `11` and
  the aliases `server`/`usridpwd`/`plaintext`/`secmec3`, `usrencpwd`/
  `encrypted_password_security`/`secmec7`, `server_encrypt`/`eusridpwd`/
  `dh_encrypted_password`/`secmec9`, `kerberos`/`gssapi`/`gssplugin`/`secmec11`.
  An explicit mechanism MUST never be silently replaced by another one.
- **DSN-05** `authentication=server|server_encrypt|kerberos` MAY be used as a
  mechanism shorthand where the driver advertises it. It MUST NOT be confused
  with `security=SSL`, which enables TLS.
- **DSN-06** A `db2s://` URL or `security=SSL` enables TLS. Supplying a CA,
  client certificate or client key also enables TLS. A TLS configuration that
  cannot be verified MUST fail before credentials are sent.
- **DSN-07** When protocol limits are configurable, they MUST use these keys
  with the same validation rules as section 4: `max_dss_size`,
  `max_ddm_object_size`, `max_reply_chain_bytes`, `max_reply_chain_packets`,
  `max_materialized_lob_size`, `max_streamed_lob_bytes`, `lob_chunk_bytes`.
  Drivers SHOULD expose at least `max_materialized_lob_size` and
  `max_streamed_lob_bytes`. A driver that does not expose a limit MUST document
  that in its status file.
- **DSN-08** Unknown keys that begin with `krb5`, `kerberos` or `gssapi` MUST fail
  closed with a configuration error; silently ignoring a misspelled credential
  option is forbidden. Other unknown keys MAY be retained as opaque parameters
  or ignored, but MUST NOT be reinterpreted as a known key.
- **DSN-09** A parsed configuration dump (if provided) MUST redact `password`,
  `pwd`, tokens and any key material. File paths to keytabs, CAs and
  certificates MAY be shown.
- **DSN-10** `database` and `user` are required for password mechanisms;
  `user` MAY be omitted for Kerberos. A missing required value MUST fail before
  opening a socket.

## 4. Resource limits

- **LIM-01** Every limit has units of bytes or packets, a documented default and
  a documented hard ceiling. Values must be positive integers; zero, negative,
  non-numeric or above-ceiling values MUST be rejected with a `configuration`
  error, not clamped silently.
- **LIM-02** The default limits MUST be:

  | Limit | Default | Unit |
  | --- | ---: | --- |
  | `max_dss_size` (single DSS payload) | 64 MiB | bytes |
  | `max_ddm_object_size` (single DDM object body) | 64 MiB | bytes |
  | `max_reply_chain_bytes` (one reply chain) | 256 MiB | bytes |
  | `max_reply_chain_packets` (one reply chain) | 4096 | packets |
  | `max_materialized_lob_size` | 16 MiB | bytes |
  | `max_materialized_result_bytes` | 32 MiB | bytes |
  | `max_streamed_lob_bytes` | 1 GiB | bytes |
  | `lob_chunk_bytes` | 64 KiB | bytes |

- **LIM-03** The hard ceilings MUST be at least the defaults and at most:

  | Limit | Hard ceiling |
  | --- | ---: |
  | `max_dss_size` | 1 GiB |
  | `max_ddm_object_size` | 1 GiB |
  | `max_reply_chain_bytes` | 4 GiB |
  | `max_reply_chain_packets` | 65535 |
  | `max_materialized_lob_size` | 1 GiB |
  | `max_materialized_result_bytes` | 1 GiB |
  | `max_streamed_lob_bytes` | 1 TiB |
  | `lob_chunk_bytes` | 16 MiB |

- **LIM-04** A materialized LOB above `max_materialized_lob_size` MUST fail with
  a `resource-limit` error and MUST NOT grow the allocation further. Values above
  the materialized limit MUST be available through the progressive/streamed API
  when the driver advertises one.
- **LIM-05** A materialized query result above `max_materialized_result_bytes`
  MUST fail with `resource-limit` before the next row allocation, not after the
  process has already exceeded the budget.
- **LIM-06** A reply chain above either reply-chain cap, or a DSS/DDM object
  above its cap, MUST fail with `resource-limit` or `protocol` as appropriate and
  MUST discard the session if the stream position is uncertain.
- **LIM-07** Batch and import staging limits follow section 9. Their byte limits
  are soft boundaries: one row larger than the byte limit is sent alone, but the
  row itself is still subject to the LOB/result limits.
- **LIM-08** Spool budgets, where the driver implements a shared spool, MUST be
  bounded, reserved before use, released on success, error and cancellation, and
  MUST NOT be reset by a failed cleanup.

## 5. Connection pool

- **POOL-01** The default pool configuration MUST be:

  | Option | Default |
  | --- | ---: |
  | `maxConnections` | 10 |
  | `maxWaiters` | 100 |
  | `acquireTimeout` | 30 s |
  | `connectTimeout` | 10 s |
  | `idleTimeout` | 300 s |
  | `maxLifetime` | 1800 s |
  | `reapInterval` | 30 s |

- **POOL-02** `maxConnections` MUST be a positive integer; `maxWaiters` MUST be
  non-negative; all intervals MUST be positive and timer-representable. Invalid
  values MUST fail with `configuration` before any connection is opened.
- **POOL-03** Exhausting the waiter bound MUST fail with `pool-exhausted`; an
  acquire deadline MUST fail with `pool-timeout`. Both are `not-started`.
- **POOL-04** A lease MUST reset session state before reuse: current schema,
  current isolation and client-info registers return to the connection baseline;
  prepared-state that cannot be proven safe is discarded. A failed reset MUST
  discard the physical session.
- **POOL-05** A lease whose operation outcome is `unknown`, whose transport is
  uncertain, or whose cancellation was not confirmed MUST NOT return to the pool.
- **POOL-06** `drainIdle`, `shutdown` and `close` MUST be idempotent and bounded.
  Active leases are not silently killed by `drainIdle`; `shutdown` closes idle
  sessions immediately and stops new acquisitions.
- **POOL-07** Pool statistics MUST expose at least total connections, idle,
  active, waiters, discarded sessions and acquire timeouts. Counters are
  monotonic for the pool lifetime.

## 6. Session verbs

- **SES-01** `connect` establishes the DRDA handshake and applies the DSN
  baseline. `close` is idempotent; closing an already closed session is a no-op.
- **SES-02** `prepare` accepts positional `?` markers and returns a statement
  handle. `execute`/`query` accept the same parameter ordering. A prepared
  statement MUST remain valid until the session is closed, the schema/isolation
  context changes, or the driver documents invalidation.
- **SES-03** `commit` and `rollback` apply to the current transaction. `rollback`
  without an active transaction is a no-op or a server-confirmed result, not a
  fabricated failure. `close` MUST NOT commit an open transaction implicitly;
  the driver MUST roll it back or discard the session.
- **SES-04** `change_database` reconnects the session to another database on the
  same server. It MUST fail before any I/O for an empty/invalid name, MUST refuse
  while a transaction is active or on a pooled lease, and MUST leave the session
  closed (never half-connected) if the reconnect fails. It MUST NOT silently
  reconnect to the old database.
- **SES-05** `set_current_schema` trims and validates a non-empty schema, applies
  `SET CURRENT SCHEMA` with a quoted identifier and invalidates prepared state.
  The pool reset restores the connection baseline schema.
- **SES-06** `current_schema` returns the schema last set by the driver, or the
  empty string/`None` when unset. It MUST NOT claim a server default the driver
  did not set.
- **SES-07** Isolation control, whether session-level or transaction-scoped,
  accepts `UR`, `CS`, `RS`, `RR` and the driver's documented aliases. A
  session-level setter also accepts `RESET`; it applies `SET CURRENT ISOLATION`
  with the same quoting/validation discipline as schema, and the pool reset
  restores the baseline isolation. A transaction-scoped control must restore the
  previous isolation when the transaction ends.
- **SES-08** Transaction state is per session, not per pool or per driver. A
  command timeout, cancellation or failed reset MUST NOT make an open transaction
  appear committed.
- **SES-09** `execute_batch` preserves parameter-set order and stops at the first
  failure. The returned affected-row count includes only server-acknowledged
  sets; partial progress is reported as specified in ERR-08 and OUT-08.
- **SES-10** Every public verb that can block MUST accept the driver's
  cancellation/deadline mechanism and MUST settle or discard the session as
  specified in section 8. Sync-only drivers may omit an async variant but must
  still expose cancellation.

## 7. Value mapping

- **VAL-01** SQL NULL is distinct from empty string, zero and empty binary. A
  decoder MUST NOT coalesce them.
- **VAL-02** Integer types preserve their value exactly. A value outside the
  language's exact integer range MUST be returned as an exact string or raise a
  conversion error; silent truncation is forbidden.
- **VAL-03** REAL/DOUBLE preserve IEEE-754 values. NaN and infinities are
  preserved or explicitly converted with a documented rule.
- **VAL-04** DECIMAL and DECFLOAT MUST be lossless. A driver MAY return a
  language decimal type or an exact canonical decimal string; binary floating
  point is forbidden. DECFLOAT special values (NaN, Infinity, -Infinity) MUST be
  preserved. Scale and precision must not be rewritten.
- **VAL-05** DATE, TIME and TIMESTAMP use the language date/time type or an ISO
  string (`YYYY-MM-DD`, `HH:MM:SS`, `YYYY-MM-DDTHH:MM:SS[.ffffff]`). Db2 temporal
  values carry no implicit timezone; a driver MUST NOT invent one. Fractional
  seconds are preserved to the column scale.
- **VAL-06** BINARY/VARBINARY/BLOB return bytes without text transcoding.
- **VAL-07** Character data is decoded as UTF-8 unless the server declares another
  encoding. CP500/EBCDIC is decoded explicitly when the server selects it.
  CHAR trailing padding and VARCHAR content are preserved; empty and NULL remain
  distinct. CLOB/DBCLOB/XML may be returned as text or a streamed value.
- **VAL-08** Column metadata MUST report name, type, precision, scale, nullable,
  length and (when available) display size. Metadata must describe the value the
  driver actually returns.
- **VAL-09** Progressive LOB streams yield the value in order, without gaps or
  duplication, and expose the final length or an end marker. Aborting a stream
  MUST NOT produce a truncated value that looks complete.
- **VAL-10** Number and temporal formatting/parsing uses invariant culture or
  explicit locale-independent rules; the process locale must not change wire
  values or decoded results.

## 8. Cancellation

- **CAN-01** Cancellation is a two-step protocol: send the DRDA SQL interrupt
  (normally on a second connection) and settle the original operation. Returning
  an acknowledgement is NOT proof that the command stopped.
- **CAN-02** The original operation MUST still be awaited/settled. A cancel call
  MUST NOT leave a background reader racing the next command on the same
  session.
- **CAN-03** A confirmed cancellation MUST surface as SQLCODE `-952` /
  SQLSTATE `57014` with kind `operation-cancelled` and outcome `confirmed`.
- **CAN-04** If the operation completes before the interrupt takes effect, the
  driver MUST return the successful result, not a fabricated cancellation error.
- **CAN-05** If the interrupt is not acknowledged or the original operation
  cannot be settled within the documented settlement deadline (default 30 s),
  the session MUST be discarded; it MUST NOT return to the pool.
- **CAN-06** Cancelling before any request is written is `not-started` and MUST
  NOT corrupt the session. Cancelling an idle session is a no-op or a
  `not-started` cancellation, never an `unknown` outcome.
- **CAN-07** Cancellation is idempotent: a second cancel while the first is
  settling must not send a second interrupt or deadlock.
- **CAN-08** A command/connect deadline uses the same interrupt/settlement path.
  A timeout that cannot be settled is `timeout` with outcome `unknown`; a
  confirmed server timeout is `sql-server` or `timeout` with `confirmed`.
- **CAN-09** Cancellation MUST be safe from another task/thread while the
  operation lock is held by the running command. After a confirmed `-952`, the
  session may be reused; after an unconfirmed cancel, it must not.

## 9. Batch, import and large objects

- **BAT-01** `execute_batch`/`executemany` runs parameter sets in order and stops
  at the first failure. It reports the completed set count, affected rows and the
  failing index as required by ERR-08 and OUT-08.
- **BAT-02** Batch execution MUST be bounded by rows and encoded bytes. Defaults
  are 256 rows and 8 MiB; `maxRows` must be in `1..32767` (Db2's 16-bit row
  count). The byte limit is soft: one oversized row is sent alone.
- **BAT-03** A batch failure wrapper MUST preserve the cause and MUST NOT report
  unacknowledged work as completed.
- **BAT-04** Import supports an atomic policy (one transaction, all rows visible
  together) and a commit-per-batch policy. A table lock is valid only with the
  atomic policy; combining it with commit-per-batch must fail during validation,
  before any SQL is sent.
- **BAT-05** Import defaults are 256 rows, 8 MiB staged bytes, progress every
  10,000 rows, atomic policy, table lock off. Staging is bounded and released on
  success, error and cancellation. An interrupted commit-per-batch import keeps
  only confirmed checkpoints; an interrupted atomic import rolls back.
- **BAT-06** A streamed LOB parameter is bounded by `max_streamed_lob_bytes`
  (default 1 GiB) and is sent in `lob_chunk_bytes` chunks (default 64 KiB).
  Exceeding the budget MUST fail with `resource-limit` before the request is
  written when the size is known, and MUST leave the session usable.
- **BAT-07** Materialized LOB and result limits follow LIM-04/LIM-05. A result
  that exceeds the result limit MUST fail closed; a LOB above the materialized
  limit must be streamed or rejected, never silently truncated.
- **BAT-08** A progressive LOB fetch exposes a cancel handle/abort signal.
  Aborting it poisons/discards the session according to the cancellation rules;
  a half-read LOB must not be reported as a complete value.
- **BAT-09** Large values above the materialized limits MUST use progressive
  locator streaming when the driver advertises it. The 1 GiB qualification
  workload is the reference: streamed bytes must checksum-match the source.

## Annex A — idiomatic realization (non-normative)

The contract is implemented with each language's own names. This table is a
navigation aid, not an additional requirement:

| Area | C# (.NET) | TypeScript | Rust | Python |
| --- | --- | --- | --- | --- |
| Error kind | `Db2ErrorKind` | `Db2ErrorCategory` | `Db2ErrorKind` | `ErrorKind` |
| Outcome | `Db2OperationOutcome` | `Db2OperationOutcome` | `Db2OperationOutcome` | `Error.outcome` |
| DSN parsing | `Db2Config.Parse` | `parseDsn` / `Config` | `Config::parse_dsn` | `Config` / `parse_dsn` |
| Limits | `Db2DssStream`, `DssReadBuffer`, `Db2Session` constants | `DSS_LIMITS`, `DB2_RESOURCE_DEFAULTS` | `ProtocolLimits` | `ProtocolLimits` |
| Pool | `Db2Pool`, `Db2PoolOptions` | `Db2Pool`, `Db2PoolOptions` | `Db2Pool`, `Db2PoolOptions` | `Pool`, `PoolOptions` |
| Batch | `Db2Session.ExecuteBatchAsync` | `executeBatch` | `Db2BatchOptions` | `execute_batch` / `BatchOptions` |
| Import | `Db2AdoImporter`, `Db2AdoImportOptions` | `Db2ImportOptions` | `Db2ImportOptions` | `import_rows` / `ImportOptions` |
| Cancellation | `CancelAsync` | `cancel` / `AbortSignal` | `Db2InterruptHandle` | `interrupt` / `CommandOptions` |
| Values | exact strings; ADO may return `decimal`/`DateTime` | exact strings | exact strings | DB-API `Decimal`/date/time |

## Compatibility and exceptions

A driver may omit a feature only where this document marks it optional, or where
a language/platform limitation makes it impossible. Every omission MUST be listed
in `docs/DRIVER-CONTRACT-STATUS.md` with the requirement ID, the reason and the
planned resolution. Silence is not an exception.

The status document is repo-specific and is not part of the normative text; the
normative text is identical in every public driver repository. The contract
describes observable behaviour only. Class names, enum spellings, option-type
names and async idioms are implementation choices and may differ.

## Change process

1. A behaviour change to the contract is proposed against this document, not
   against one driver.
2. Additive clarifications increment the v1.x version and keep all existing
   requirement IDs.
3. Breaking changes require JDC v2 and a migration note in every driver.
4. A driver's status file is updated in the same change that closes or opens a
   gap.
