# Benchmark methodology

This repository keeps reproducible benchmark code, not historical raw performance dumps from private development machines.

## Principles

- Correctness and protocol synchronization take precedence over micro-optimizations.
- Replay benchmarks measure deterministic client-side protocol/decode work and do not substitute for live Db2 measurements.
- Live benchmarks include server/network variance and must record the exact source revision, Rust toolchain, Db2 version, query shape, row count, and relevant block/pool settings.
- Performance claims should be based on repeated measurements and should not generalize synthetic no-op writers to end-to-end network throughput.
- Allocation optimizations must preserve bounded buffering and resource limits.

## Offline benchmarks

```bash
cargo bench --locked --features bench-internals --bench hot_paths
cargo bench --locked --bench import
cargo bench --locked --bench replay
```

`benches/replay.rs` exercises deterministic loopback DRDA fixtures at the maintained block-size matrix. `benches/hot_paths.rs` targets codec/parser hot paths and `benches/import.rs` covers bounded import encoding.

## Live benchmarks

Live reader/pool/write benchmarks require `DB2_TEST_DSN` and are intentionally separate from deterministic replay. Run them only against an isolated benchmark database and report the exact environment with the result.

```bash
cargo bench --locked --bench reader
cargo bench --locked --bench pool_write
```

## Release policy

Benchmark results are advisory. Release qualification is defined by `docs/QUALIFICATION.md`; no production default is changed from replay or a single benchmark run alone.
