use std::{env, error::Error, time::Instant};

use justybase_db2::{Db2ClientInfo, Db2Pool, Db2Value, PoolResetPolicy, parse_dsn};

#[derive(Clone, Copy)]
enum Workload {
    CleanCheckout,
    Query,
    Schema,
    ClientInfo,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::CleanCheckout => "clean-checkout-release",
            Self::Query => "values-query-release",
            Self::Schema => "typed-schema-reset",
            Self::ClientInfo => "client-info-reset",
        }
    }
}

async fn benchmark_policy(
    dsn: &str,
    policy: PoolResetPolicy,
    iterations: usize,
) -> Result<(), Box<dyn Error>> {
    let pool = Db2Pool::new_with_reset_policy(parse_dsn(dsn)?, 1, policy)?;
    pool.acquire().await?.release().await?;

    for workload in [
        Workload::CleanCheckout,
        Workload::Query,
        Workload::Schema,
        Workload::ClientInfo,
    ] {
        let started = Instant::now();
        for _ in 0..iterations {
            let mut lease = pool.acquire().await?;
            match workload {
                Workload::CleanCheckout => {}
                Workload::Query => {
                    let result = lease.query("VALUES 1").await?;
                    if result.rows != [vec![Db2Value::Integer(1)]] {
                        return Err("pool benchmark query returned an unexpected value".into());
                    }
                }
                Workload::Schema => lease.set_current_schema("SYSIBM").await?,
                Workload::ClientInfo => {
                    lease
                        .set_client_info(Db2ClientInfo {
                            application_name: "RUST_DB2_POOL_BENCH".into(),
                            ..Db2ClientInfo::default()
                        })
                        .await?;
                }
            }
            lease.release().await?;
        }
        let elapsed = started.elapsed();
        println!(
            "policy={policy:?} workload={} iterations={} elapsed_ms={:.3} operations_per_second={:.1}",
            workload.name(),
            iterations,
            elapsed.as_secs_f64() * 1000.0,
            iterations as f64 / elapsed.as_secs_f64(),
        );
    }
    pool.close().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let iterations = env::args()
        .nth(1)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(20);
    if iterations == 0 {
        return Err("iterations must be positive".into());
    }
    let dsn = env::var("DB2_TEST_DSN")?;
    benchmark_policy(&dsn, PoolResetPolicy::Strict, iterations).await?;
    benchmark_policy(&dsn, PoolResetPolicy::Tracked, iterations).await?;
    Ok(())
}
