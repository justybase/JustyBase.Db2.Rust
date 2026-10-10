//! Pool disposal / failure replay tests.
use justybase_db2::replay;

#[tokio::test]
async fn dropped_unfinished_transaction_is_not_clean() {
    let (port, server) = replay::spawn_replay_server(replay::ReplayFixture::Small, 1, 65535).await;
    let mut session = replay::connect_replay(port).await;
    let tx = session.begin_transaction().unwrap();
    drop(tx);
    // An unfinished transaction must not leave the session silently reusable;
    // `Db2Transaction::drop` marks the session closed, so the next use must
    // fail closed.
    let result = session.query("VALUES 1").await;
    assert!(
        result.is_err(),
        "query after dropped transaction must fail closed"
    );
    assert!(
        session.is_closed(),
        "unfinished transaction must not be treated as clean"
    );
    server.abort();
}

#[tokio::test]
async fn failed_cleanup_invalidates_session() {
    let (port, server) =
        replay::spawn_replay_server(replay::ReplayFixture::Discard, 5000, 1024).await;
    let mut session = replay::connect_replay(port).await;
    {
        let mut reader = session.query_stream("SELECT ID FROM REPLAY").await.unwrap();
        let _ = reader.next().await.unwrap();
    }
    assert!(session.is_closed());
    server.abort();
}
