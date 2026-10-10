//! Live early-terminate tests. Require `DB2_TEST_DSN`; ignored offline.
//!
//! Run serially: `cargo test --locked --test discard_live -- --ignored --test-threads=1`.
#![cfg(test)]

use justybase_db2::Db2Session;

fn dsn() -> Option<String> {
    std::env::var("DB2_TEST_DSN").ok()
}

#[tokio::test]
#[ignore]
async fn live_read1_then_next_query_succeeds() {
    let dsn = dsn().expect("DB2_TEST_DSN required");
    let mut session = Db2Session::connect_dsn(&dsn).await.unwrap();
    session.query("VALUES 1").await.unwrap();
    let mut reader = session
        .query_stream(
            "SELECT ID FROM P1BENCH.INT_1M ORDER BY ID FETCH FIRST 1000 ROWS ONLY WITH UR",
        )
        .await
        .unwrap_or_else(|_| panic!("P1BENCH.INT_1M missing; create the P1 corpus first"));
    let first = reader.next().await.unwrap().unwrap();
    assert!(!first.is_empty());
    reader.close().await.unwrap();
    // The connection must be reusable after an early close.
    let v = session.query("VALUES 2").await.unwrap();
    assert_eq!(v.rows.len(), 1);
    session.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn live_read10_then_close_and_reuse() {
    let dsn = dsn().expect("DB2_TEST_DSN required");
    let mut session = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut reader = session
        .query_stream(
            "SELECT ID FROM P1BENCH.INT_1M ORDER BY ID FETCH FIRST 1000 ROWS ONLY WITH UR",
        )
        .await
        .unwrap();
    for _ in 0..10 {
        reader.next().await.unwrap().unwrap();
    }
    reader.close().await.unwrap();
    let v = session.query("VALUES 3").await.unwrap();
    assert_eq!(v.rows.len(), 1);
    session.close().await.unwrap();
}
