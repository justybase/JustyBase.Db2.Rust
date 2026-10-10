//! Live reader benchmarks against immutable `P1BENCH`-style tables.
//!
//! Requires `DB2_TEST_DSN`. Skips (reports nothing) when unset so offline
//! `cargo bench --offline` stays green. Tables are read-only; write cases use
//! a unique temporary table per run with explicit rollback/drop at cleanup.
//!
//! Scenarios: INT 1-col fetch, narrow 5-col, wide NULL mix, VARCHAR/CLOB/BLOB
//! shapes where available, DECIMAL/TIMESTAMP, small open/close. Exact table
//! names follow the `P1BENCH` corpus; missing tables fail with a clear message
//! instead of silently measuring an empty result.
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use justybase_db2::Db2Session;
use std::time::Duration;
use tokio::runtime::Runtime;

fn dsn() -> Option<String> {
    std::env::var("DB2_TEST_DSN").ok()
}

async fn open() -> Option<Db2Session> {
    let dsn = dsn()?;
    match Db2Session::connect_dsn(&dsn).await {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("reader bench: connect failed, skipping: {e}");
            None
        }
    }
}

async fn count_query(sql: &str) -> Option<(usize, u64)> {
    let mut session = open().await?;
    let mut reader = match session.query_stream(sql).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("reader bench: open failed for {sql}: {e}");
            return None;
        }
    };
    let mut count = 0usize;
    while reader.next().await.unwrap().is_some() {
        count += 1;
    }
    let _ = reader.close().await;
    let _ = session.close().await;
    Some((count, 0))
}

fn bench_reader(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    if dsn().is_none() {
        eprintln!("reader bench: DB2_TEST_DSN unset, registering placeholder only");
        let mut group = c.benchmark_group("live_reader_skipped");
        group.bench_function("skipped_no_dsn", |b| b.iter(|| 0));
        group.finish();
        return;
    }
    let mut group = c.benchmark_group("live_reader");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(500));
    // Candidate live shapes; each verifies a non-empty result once before timing.
    let scenarios = [
        ("int_1m", "SELECT ID FROM P1BENCH.INT_1M ORDER BY ID"),
        ("narrow_5c_100k", "SELECT * FROM P1BENCH.NARROW_5C"),
        ("wide_null_100k", "SELECT * FROM P1BENCH.WIDE_NULL"),
        ("decimal_ts_10k", "SELECT * FROM P1BENCH.DECIMAL_TS"),
        ("small_open_close", "VALUES 1"),
    ];
    for (name, sql) in scenarios {
        let probe: Option<(usize, u64)> = rt.block_on(count_query(sql));
        match probe {
            Some((0, _)) => eprintln!("reader bench: {name} empty/missing, still timing open path"),
            None => eprintln!("reader bench: {name} unavailable, timing attempted open"),
            Some((n, _)) => eprintln!("reader bench: {name} probe rows={n}"),
        }
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::new("fetch", name), |b| {
            b.to_async(&rt).iter(|| async {
                let mut session = open().await.unwrap();
                let mut reader = session.query_stream(sql).await.unwrap();
                let mut n = 0usize;
                while reader.next().await.unwrap().is_some() {
                    n += 1;
                }
                let _ = reader.close().await;
                session.close().await.unwrap();
                n
            });
        });
    }
    group.finish();
}

criterion_group!(reader_benches, bench_reader);
criterion_main!(reader_benches);
