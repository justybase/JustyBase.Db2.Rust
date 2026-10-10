//! Early-terminate replay tests: Read1/Read10 + close, scalar drain.
use justybase_db2::replay::{self, ReplayFixture};

#[tokio::test]
async fn replay_read1_then_close_succeeds() {
    let (port, server) = replay::spawn_replay_server(ReplayFixture::Discard, 1000, 16384).await;
    let mut session = replay::connect_replay(port).await;
    let mut reader = session.query_stream("SELECT ID FROM REPLAY").await.unwrap();
    let first = reader.next().await.unwrap().unwrap();
    assert_eq!(first[0], justybase_db2::Db2Value::Integer(1));
    reader.close().await.unwrap();
    session.close().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn replay_read10_then_close_succeeds() {
    let (port, server) = replay::spawn_replay_server(ReplayFixture::Discard, 1000, 16384).await;
    let mut session = replay::connect_replay(port).await;
    let mut reader = session.query_stream("SELECT ID FROM REPLAY").await.unwrap();
    for expected in 1..=10 {
        let row = reader.next().await.unwrap().unwrap();
        assert_eq!(row[0], justybase_db2::Db2Value::Integer(expected));
    }
    reader.close().await.unwrap();
    session.close().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn replay_dropped_reader_invalidates_session_fail_closed() {
    // Dropping without close cannot perform async cursor cleanup; the session
    // must not be silently reused. Use tiny blocks so ENDQRYRM has not arrived
    // after the first row (multi-block cursor still open).
    let (port, server) = replay::spawn_replay_server(ReplayFixture::Discard, 5000, 1024).await;
    let mut session = replay::connect_replay(port).await;
    {
        let mut reader = session.query_stream("SELECT ID FROM REPLAY").await.unwrap();
        let _ = reader.next().await.unwrap().unwrap();
        // Intentionally drop without close.
    }
    assert!(
        session.is_closed(),
        "dropped unread cursor must invalidate the session"
    );
    server.abort();
}

#[tokio::test]
async fn replay_small_single_row_round_trip() {
    let (port, server) = replay::spawn_replay_server(ReplayFixture::Small, 1, 65535).await;
    let mut session = replay::connect_replay(port).await;
    let (count, sum) = replay::drain_stream_checksum(&mut session, "SELECT 1 FROM REPLAY").await;
    assert_eq!(count, 1);
    assert_eq!(sum, 1);
    session.close().await.unwrap();
    server.abort();
}
