//! Deterministic DRDA replay benchmarks over loopback TCP.
//!
//! Each case spawns a synthetic replay peer (see `src/replay.rs`) serving a
//! precomputed `QRYDTA` fixture split at the requested `QRYBLKSZ`. The timed
//! section covers per-iteration peer spawn + client connect/handshake + fetch
//! + decode + row iteration; peer work runs in another task and is excluded
//!   from client timing except for TCP scheduling variance. Fixture checksums
//!   are validated inside timing via asserts; keep sample counts small so
//!   handshake cost does not dominate short cases.
//!
//! This is a client stress model: one `QRYDTA` block per fetch. It does not
//! emulate `MAXBLKEXT` extension scheduling, `SQLINTR`, progressive locators,
//! or procedure cursors. Live Db2 suites remain the normative gate before any
//! production fetch-policy change. Default `QRYBLKSZ 65535` is unchanged.
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use justybase_db2::replay::{self, ReplayFixture};
use std::time::Duration;
use tokio::runtime::Runtime;

fn bench_fixture(c: &mut Criterion, rt: &Runtime, fixture: ReplayFixture, block_size: usize) {
    let rows = fixture.bench_rows();
    let mut group = c.benchmark_group(format!("replay_{}_{}", fixture.name(), block_size));
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(rows as u64));

    let expected_sum = replay::checksum(rows);

    // Read-all: full stream consumption, touching every INTEGER value in
    // each row (differs from `first_column` on multi-column fixtures).
    group.bench_function(BenchmarkId::new("read_all", fixture.name()), |b| {
        b.to_async(rt).iter(|| async {
            let (port, server) = replay::spawn_replay_server(fixture, rows, block_size).await;
            let mut session = replay::connect_replay(port).await;
            let mut reader = session.query_stream("SELECT ID FROM REPLAY").await.unwrap();
            let mut count = 0usize;
            let mut sum = 0u64;
            while let Some(row) = reader.next().await.unwrap() {
                count += 1;
                for value in row.iter() {
                    if let justybase_db2::Db2Value::Integer(v) = value {
                        sum += *v as u64;
                    }
                }
            }
            reader.close().await.unwrap();
            assert_eq!(count, rows);
            // Each row holds value `row` in every column; the per-value sum is
            // `checksum(rows) * columns`.
            assert_eq!(sum, expected_sum * fixture.columns() as u64);
            session.close().await.unwrap();
            server.abort();
        });
    });

    // First-column projection: iterate all rows but touch only the first value.
    // Identical to `read_all` on single-column fixtures by construction.
    group.bench_function(BenchmarkId::new("first_column", fixture.name()), |b| {
        b.to_async(rt).iter(|| async {
            let (port, server) = replay::spawn_replay_server(fixture, rows, block_size).await;
            let mut session = replay::connect_replay(port).await;
            let mut reader = session.query_stream("SELECT ID FROM REPLAY").await.unwrap();
            let mut count = 0usize;
            let mut sum = 0u64;
            while let Some(row) = reader.next().await.unwrap() {
                count += 1;
                if let Some(justybase_db2::Db2Value::Integer(v)) = row.first() {
                    sum += *v as u64;
                }
            }
            reader.close().await.unwrap();
            assert_eq!(count, rows);
            assert_eq!(sum, expected_sum);
            session.close().await.unwrap();
            server.abort();
        });
    });

    // Early dispose after 1 row: drops the reader without `close()`, which
    // fail-closes the session (no protocol drain; see `Db2RowReader::drop`).
    // Group throughput still reports fixture elements, so divide by `rows`
    // before comparing against `read_all`; elapsed time covers open + first
    // fetch + drop only.
    group.bench_function(BenchmarkId::new("read1_dispose", fixture.name()), |b| {
        b.to_async(rt).iter(|| async {
            let (port, server) = replay::spawn_replay_server(fixture, rows, block_size).await;
            let mut session = replay::connect_replay(port).await;
            let mut reader = session.query_stream("SELECT ID FROM REPLAY").await.unwrap();
            let first = reader.next().await.unwrap().unwrap();
            assert_eq!(first[0], justybase_db2::Db2Value::Integer(1));
            drop(reader);
            session.close().await.unwrap();
            server.abort();
        });
    });

    // Early dispose after 10 rows.
    group.bench_function(BenchmarkId::new("read10_dispose", fixture.name()), |b| {
        b.to_async(rt).iter(|| async {
            let (port, server) = replay::spawn_replay_server(fixture, rows, block_size).await;
            let mut session = replay::connect_replay(port).await;
            let mut reader = session.query_stream("SELECT ID FROM REPLAY").await.unwrap();
            for _ in 0..10.min(rows) {
                reader.next().await.unwrap().unwrap();
            }
            drop(reader);
            session.close().await.unwrap();
            server.abort();
        });
    });

    group.finish();
}

fn bench_replay(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    // Main parity matrix at the default block size.
    for fixture in [
        ReplayFixture::Int,
        ReplayFixture::Mixed,
        ReplayFixture::Wide,
        ReplayFixture::Discard,
        ReplayFixture::Small,
    ] {
        bench_fixture(c, &rt, fixture, 65535);
    }
    // Block-size scaling on the Int fixture (diagnostic, not acceptance).
    for block_size in [16384usize, 32767] {
        bench_fixture(c, &rt, ReplayFixture::Int, block_size);
    }
}

criterion_group!(replay_benches, bench_replay);
criterion_main!(replay_benches);
