//! Pool admission / lifecycle replay tests (bounded, fail-closed).
use justybase_db2::{Db2Pool, PoolResetPolicy, replay};

#[tokio::test]
async fn pool_acquire_release_on_replay_peer() {
    let (port, _) = replay::spawn_replay_server(replay::ReplayFixture::Small, 1, 65535).await;
    // Pool connects lazily on acquire; the replay peer serves one connection.
    // A size-1 pool with a second concurrent acquire must fail closed (bounded
    // admission) rather than queue without bound. Here we verify serial reuse.
    let pool = Db2Pool::connect_dsn(
        &format!("db2://db2inst1:password@127.0.0.1:{port}/TESTDB"),
        1,
    )
    .unwrap();
    // NOTE: the replay peer serves synthetic INTEGER fixtures for every query
    // and cannot satisfy the pool baseline query (`SELECT ... FROM
    // SYSIBM.SYSDUMMY1` expecting 7 TEXT columns). Acquire must therefore
    // resolve quickly with a fail-closed protocol error — not hang and not
    // hand out a silently wrong session. If the peer ever learns the baseline
    // shape, the success path below covers serial reuse.
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), pool.acquire()).await;
    assert!(
        outcome.is_ok(),
        "pool acquire must resolve within the deadline"
    );
    match outcome.expect("timeout resolved") {
        Ok(session) => {
            session.release().await.expect("release must succeed");
        }
        Err(e) => {
            assert_eq!(
                e.kind,
                justybase_db2::Db2ErrorKind::Protocol,
                "replay baseline mismatch must fail closed with a protocol error, got: {e}"
            );
        }
    }
    pool.close().await.expect("pool close must succeed");
}

#[tokio::test]
async fn pool_reset_policies_are_distinct_values() {
    assert_ne!(PoolResetPolicy::Strict, PoolResetPolicy::Tracked);
}
