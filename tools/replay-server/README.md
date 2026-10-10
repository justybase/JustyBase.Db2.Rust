# Deterministic DRDA replay (justybase-db2)

Loopback-only synthetic DRDA peer for `benches/replay.rs` and offline
regression tests (`tests/replay_*.rs`, `tests/reader_cleanup_error.rs`).
It does not require Db2, credentials, captured authentication exchanges, or
mutable database state. It is not a general Db2 emulator or protocol
reference.

Implementation lives in `src/replay.rs` (shared fixture + server helpers) so
benches and integration tests use the same handshake, open, fetch, and close
paths. The peer validates `EXCSAT`/`ACCSEC`, `SECCHK`/`ACCRDB`, session setup
(`EXCSQLSET`), prepare/open (`PRPSQLSTT`/`OPNQRY`), cursor fetch (`CNTQRY`),
and close (`CLSQRY`/`RDBCMM`). Unexpected requests get a fail-closed `SQLCARD`
instead of hanging.

Fixtures use IDs starting at 1 with checksum `N * (N + 1) / 2` per column:

| Name    | Bench rows | Columns            |
| ------- | ---------: | ------------------ |
| Int     |     20,000 | 1 `INTEGER`        |
| Mixed   |     10,000 | 2 `INTEGER`        |
| Wide    |      2,000 | 10 `INTEGER`       |
| Discard |     10,000 | 2 `INTEGER`        |
| Small   |          1 | 1 `INTEGER`        |

Reference full counts (`1M/100K/250K`) mirror the cs-db2 replay family and are
documented in `ReplayFixture::reference_rows`; timed Criterion iterations use
the smaller bench counts above so loopback runs finish in seconds. Each bench
iteration spawns its own loopback peer, so per-iteration spawn +
connect/handshake cost is included in timing; peer work itself runs in another
task. The client retains fixture formulas and metadata, not a row
corpus.

Fetch shapes are precomputed at 16384, 32767, and 65535 bytes. Splitting a
byte stream deliberately exercises partial rows. This is a client stress
model: a requested `QRYBLKSZ` alone does not prove Db2 emits this exact shape
or that `MAXBLKEXT` has these synthetic semantics. Live traces and live
correctness gates are required before changing any production default.

```sh
cargo bench --locked --offline --bench replay
python3 benches/summarize-replay.py target/criterion /tmp/replay-metrics.csv
```

Peer work runs in another task
and is excluded from client timing; TCP scheduling still contributes
variance. Small-result times are short: inspect variance instead of treating
small differences as reliable improvements.

Block-size diagnostics are exploratory, not acceptance evidence:

```sh
cargo bench --locked --offline --bench replay -- replay_Int_16384 replay_Int_32767 replay_Int_65535
python3 benches/summarize-block-counters.py target/criterion /tmp/block-counters.csv
```

The peer implements one `QRYDTA` block per fetch. It deliberately does not
model Db2 LUW `MAXBLKEXT` extension scheduling, `SQLINTR`'s secondary
connection, progressive locators, prepared parameter execution, or procedure
result sets. Existing protocol tests and the live suite cover those
behaviors. Never infer a new production block-size default from replay alone.
