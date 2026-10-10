//! Live block-size probe (diagnostic, not acceptance).
//!
//! Compares `QRYBLKSZ` 16384/32767/65535 on the `P1BENCH` corpus and verifies
//! identical row counts + checksums. Never infers a new production default
//! from these numbers alone; server `MAXBLKEXT` scheduling differs from replay.
//! Require `DB2_TEST_DSN`; ignored offline. Serial only.
use justybase_db2::{Db2Session, parse_dsn};

fn dsn() -> Option<String> {
    std::env::var("DB2_TEST_DSN").ok()
}

async fn fetch_all_with_block(dsn: &str, block: usize) -> (usize, u64) {
    let mut config = parse_dsn(dsn).unwrap();
    config.block_size = block;
    let mut session = Db2Session::connect(config).await.unwrap();
    let mut reader = session
        .query_stream(
            "SELECT ID FROM P1BENCH.INT_1M ORDER BY ID FETCH FIRST 5000 ROWS ONLY WITH UR",
        )
        .await
        .unwrap();
    let mut count = 0usize;
    let mut sum = 0u64;
    while let Some(row) = reader.next().await.unwrap() {
        count += 1;
        if let justybase_db2::Db2Value::Integer(v) = row[0] {
            sum += v as u64;
        }
    }
    reader.close().await.unwrap();
    session.close().await.unwrap();
    (count, sum)
}

#[tokio::test]
#[ignore]
async fn live_block_sizes_agree_on_rows_and_checksum() {
    let dsn = dsn().expect("DB2_TEST_DSN required");
    let mut results = Vec::new();
    for block in [16384usize, 32767, 65535] {
        let (count, sum) = fetch_all_with_block(&dsn, block).await;
        eprintln!("live block probe: QRYBLKSZ={block} rows={count} checksum={sum}");
        results.push((count, sum));
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[1], results[2]);
}
