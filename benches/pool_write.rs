//! Live pool + write benchmarks. Requires `DB2_TEST_DSN`; skips offline.
//!
//! Pool matrix mirrors the cs-db2 `Db2PoolBenchmarks`: clean checkout/release,
//! `VALUES 1` + release under `Strict` vs `Tracked`, typed schema reset and
//! client-register reset. Write matrix mirrors `Db2WriteBenchmarks`: repeated
//! no-match `DELETE` and 128-row importer batches with explicit transaction
//! and rollback on a unique temporary table per run.
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use justybase_db2::{Db2ClientInfo, Db2Pool, Db2Value, PoolResetPolicy};
use std::time::Duration;
use tokio::runtime::Runtime;

fn dsn() -> Option<String> {
    std::env::var("DB2_TEST_DSN").ok()
}

fn bench_pool_write(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    if dsn().is_none() {
        eprintln!("pool_write bench: DB2_TEST_DSN unset, placeholder only");
        let mut group = c.benchmark_group("live_pool_write_skipped");
        group.bench_function("skipped_no_dsn", |b| b.iter(|| 0));
        group.finish();
        return;
    }
    let mut group = c.benchmark_group("live_pool_write");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(500));

    for policy in [PoolResetPolicy::Strict, PoolResetPolicy::Tracked] {
        let label = match policy {
            PoolResetPolicy::Strict => "strict",
            PoolResetPolicy::Tracked => "tracked",
        };
        // Steady-state checkout/release: the pool is created once per case,
        // each iteration only checks out and releases (handshake cost is
        // amortized after the first acquire).
        let pool = {
            let dsn = dsn().unwrap();
            match policy {
                PoolResetPolicy::Strict => Db2Pool::connect_dsn(&dsn, 4).unwrap(),
                PoolResetPolicy::Tracked => {
                    let config = justybase_db2::parse_dsn(&dsn).unwrap();
                    Db2Pool::new_with_reset_policy(config, 4, policy).unwrap()
                }
            }
        };
        group.bench_function(BenchmarkId::new("clean_checkout_release", label), |b| {
            b.to_async(&rt).iter(|| async {
                let session = pool.acquire().await.unwrap();
                session.release().await.unwrap();
            });
        });
        group.bench_function(BenchmarkId::new("values1_release", label), |b| {
            b.to_async(&rt).iter(|| async {
                let mut session = pool.acquire().await.unwrap();
                session.query("VALUES 1").await.unwrap();
                session.release().await.unwrap();
            });
        });
        group.bench_function(BenchmarkId::new("client_register_reset", label), |b| {
            b.to_async(&rt).iter(|| async {
                let mut session = pool.acquire().await.unwrap();
                session
                    .set_client_info(Db2ClientInfo {
                        application_name: "poolbench".into(),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                session.release().await.unwrap();
            });
        });
        let _ = rt.block_on(pool.close());
    }

    group.bench_function("write_no_match_delete", |b| {
        b.to_async(&rt).iter(|| async {
            let mut session = justybase_db2::Db2Session::connect_dsn(&dsn().unwrap())
                .await
                .unwrap();
            // Fail fast on a missing corpus table instead of timing an error path.
            let n = session
                .execute("DELETE FROM P1BENCH.WRITE_TMP WHERE ID = -1")
                .await
                .expect("P1BENCH.WRITE_TMP missing; create the write corpus first");
            assert_eq!(n, 0);
            session.close().await.unwrap();
        });
    });

    group.bench_function("write_import_128_rollback", |b| {
        b.to_async(&rt).iter(|| async {
            use std::sync::atomic::{AtomicU64, Ordering};
            static SEQ: AtomicU64 = AtomicU64::new(0);
            let mut session = justybase_db2::Db2Session::connect_dsn(&dsn().unwrap())
                .await
                .unwrap();
            // Unique per iteration: pid + monotonic counter avoids collisions
            // across Criterion's 10 iterations within one process.
            let table = format!(
                "WRITE_BENCH_{}_{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            );
            session
                .execute(&format!("CREATE TABLE {table} (ID INTEGER)"))
                .await
                .unwrap();
            let mut tx = session.begin_transaction().unwrap();
            for i in 0..128 {
                tx.execute_with_params(
                    &format!("INSERT INTO {table} VALUES (?)"),
                    &[Db2Value::Integer(i)],
                )
                .await
                .unwrap();
            }
            tx.rollback().await.unwrap();
            session
                .execute(&format!("DROP TABLE {table}"))
                .await
                .unwrap();
            session.close().await.unwrap();
        });
    });

    group.finish();
}

criterion_group!(pool_write_benches, bench_pool_write);
criterion_main!(pool_write_benches);
