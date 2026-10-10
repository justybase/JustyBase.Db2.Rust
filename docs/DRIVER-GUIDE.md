# Advanced driver guide

This guide collects operational details that are intentionally kept out of the main README. The driver is alpha software; prefer explicit validation over assumptions when integrating it into production-like workloads.

## Connection and authentication

The driver accepts URL-style Db2 DSNs and a documented subset of IBM CLI-style key/value strings. New connections use server-side password authentication by default. Unsupported authentication or security modes fail during configuration validation rather than silently changing behavior.

Use TLS for credentials in transit. The maintained TLS qualification profile validates TLS 1.2 and 1.3 behavior.

## Pooling and reset policy

`Db2Pool` uses bounded physical sessions and bounded waiter admission. The pool tracks connection lifecycle, idle/lifetime limits and shutdown state.

The default `PoolResetPolicy::Strict` validates relevant server session state before reuse. `PoolResetPolicy::Tracked` can avoid that validation round trip when an application guarantees it changes state only through the typed driver APIs.

A session is discarded instead of reused when the driver cannot prove that protocol or transactional state is synchronized. This includes uncertain cleanup after transport failures, incomplete cancellation and failed transaction control.

Returning an unfinished transaction to the pool is not treated as a clean lease.

## Row streaming

`query_stream` and `query_stream_with_params` return a pull-based `Db2RowReader`.

The reader owns the active cursor lifecycle. The caller should consume the stream or explicitly call `close().await` so the driver can drain and close server state before the session is reused. Early `close()` drains the remaining protocol exchange before reuse; errors discovered during that drain are reported and an uncertain drain invalidates the session (see `tests/replay_discard.rs`, `tests/reader_cleanup_error.rs`).

`next_ref().await` exposes a `Db2RowRef` whose text and byte values borrow storage owned by the current decoded row. Those borrowed values remain valid only until the reader advances.

Dropping an unread cursor can invalidate the transport because Rust `Drop` cannot perform the asynchronous protocol cleanup required to guarantee safe reuse. If all data plus `ENDQRYRM` already arrived, dropping is safe; otherwise the session is marked closed (fail-closed) rather than risking reuse with uncertain cursor state.

## Replay and block-size policy

Deterministic loopback replay (`tools/replay-server/`, `src/replay.rs`, `benches/replay.rs`, `docs/perf-phase0.md`, `docs/perf-replay-pass.md`) is a client stress model with one `QRYDTA` block per fetch at `16384/32767/65535` bytes. It does not prove Db2 emits those shapes and must not be used alone to change the default `Config::block_size` of `65535`. Live block probes (`tests/block_probe_live.rs`, `benches/reader.rs`) are diagnostic; production fetch policy changes require live LUW evidence including `MAXBLKEXT` behavior.

## Large objects

Db2 can represent large values in several ways. The driver distinguishes bounded materialization from progressive locator streaming.

### Materialized values

The current alpha limits:

- one materialized LOB to 16 MiB,
- one materialized query result to 32 MiB.

Values that exceed those limits fail rather than allocating without bound.

### Progressive row-stream values

For supported progressive locators, streamed rows can expose `Db2Value::Lob(Db2LobValue)`. Open the value through the row reader and consume chunks incrementally.

The returned LOB reader borrows the row reader. Consume or drain the locator before advancing the parent row stream. Skipped or partially consumed locators are drained in order before the cursor can continue.

If the server instead returns an inline or non-progressive EXTDTA representation, that value can still be materialized and is therefore subject to the materialization limits.

### Streaming inputs

Known-length BLOB/CLOB/DBCLOB/XML sources can be sent without materializing the full payload. The declared byte length is part of the protocol contract; mismatches can invalidate the operation.

## Cancellation and command deadlines

Cancellation uses Db2's DRDA interrupt mechanism over a separate connection.

The important distinction is:

1. the interrupt connection can acknowledge the request,
2. the original command is still responsible for returning its final reply,
3. only the original reply proves whether the command was cancelled, completed normally, or left the session uncertain.

A typical confirmed cancellation is reported by Db2 as SQLCODE `-952` / SQLSTATE `57014`.

The interrupt acknowledgment alone does not prove that the command has stopped. Always await the original command when the final state matters.

`Db2CommandOptions` separates the active command deadline from interrupt-acknowledgment and cleanup budgets. If cleanup cannot be confirmed, the driver closes/poisons the session rather than reusing uncertain state.

Dropping or aborting an arbitrary Rust future is ordinary Rust cancellation; it is not equivalent to a confirmed Db2 SQL interrupt exchange.

## Transactions

Transaction scopes hold exclusive mutable access to their session.

- `commit` and `rollback` restore normal session use only after successful protocol completion.
- failed transaction-control replies invalidate the session,
- an unfinished transaction is not silently reused as a clean pooled session.

Use explicit transaction completion whenever possible.

## Stored procedures and dynamic result sets

Procedure execution supports input, output and input/output parameters plus dynamic result cursors.

`call_procedure_stream` and `call_procedure_stream_with_options` expose a `Db2ProcedureReader` for cursor-by-cursor consumption.

The caller must finish or close the reader so the driver can drain remaining cursors and collect terminal OUT/INOUT data. Output values can remain unavailable until that terminal drain completes because Db2 can interleave procedure metadata, output data and cursor exchanges.

Procedure result LOBs are handled conservatively because server profiles can represent them differently from ordinary row-stream locator values.

## Import pipeline

The bounded row importer sends prepared INSERT operations. It is not IBM CLI LOAD.

The importer supports atomic and checkpoint/commit-every-rows style policies. Batches are bounded by row and estimated encoded-byte limits. Progressive source LOBs can be staged in private temporary storage under the shared process spool quota before the destination operation advances.

`import_rows_with_options` with `Db2ImportOptions` additionally supports opt-in periodic progress reports (`notify_after_rows: Option<u64>`: the observer runs each time the source row count reaches a multiple of the interval, so returning an error rolls back the atomic transaction or stops `CommitEveryRows` at its last confirmed checkpoint), an exclusive `LOCK TABLE ... IN EXCLUSIVE MODE` for atomic imports (`table_lock`, rejected with checkpoint policies), and a per-batch write deadline (`command_timeout_ms`, zero keeps the session default).

When an outcome is uncertain, confirmed progress is preserved separately from the failed row/operation state.

## Session state and client information

Typed APIs can manage current schema, isolation, client application/workstation/user/accounting fields and trusted-context identity.

Pool reset uses the tracked or strict policy to prevent caller-specific state from leaking to a later lease. If state cannot be restored safely, the connection is discarded.

Trusted user switching requires corresponding Db2 server privileges/configuration.

## Errors and warnings

`Db2Error` classifies configuration, closed-session, authentication, authorization, server SQL, transport, protocol and other failures. SQLCODE and SQLSTATE are retained when the server provides them.

Authentication errors do not include the configured DSN/password.

Successful SQL warnings can be delivered through `set_warning_handler`; returning a pooled lease clears caller-specific warning handlers so they cannot leak between users.

## Fail-closed behavior

The driver deliberately favors connection disposal over speculative reuse when protocol synchronization cannot be proven. Examples include:

- malformed reply chains,
- missing or contradictory LOB payloads,
- incomplete cursor cleanup,
- transport failures during a request/reply exchange,
- unconfirmed interrupt cleanup,
- failed transaction control,
- impossible server state transitions.

This behavior is intentional and part of the driver's resource/safety model.
