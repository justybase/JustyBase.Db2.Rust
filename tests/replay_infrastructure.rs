//! Replay infrastructure validation: handshake, open, fetch, close.
use justybase_db2::replay::{self, ReplayFixture};

#[tokio::test]
async fn replay_handshake_and_open_serve_all_fixtures() {
    for fixture in [
        ReplayFixture::Int,
        ReplayFixture::Mixed,
        ReplayFixture::Wide,
        ReplayFixture::Discard,
        ReplayFixture::Small,
    ] {
        let rows = 50;
        let (port, server) = replay::spawn_replay_server(fixture, rows, 65535).await;
        let mut session = replay::connect_replay(port).await;
        let (count, sum) =
            replay::drain_stream_checksum(&mut session, "SELECT ID FROM REPLAY").await;
        assert_eq!(count, rows, "fixture {:?}", fixture.name());
        assert_eq!(sum, replay::checksum(rows));
        session.close().await.unwrap();
        server.abort();
    }
}

#[tokio::test]
async fn replay_block_sizes_deliver_identical_rows_and_checksums() {
    for block_size in [16384usize, 32767, 65535] {
        let rows = 500;
        let (port, server) =
            replay::spawn_replay_server(ReplayFixture::Int, rows, block_size).await;
        let mut session = replay::connect_replay(port).await;
        let (count, sum) =
            replay::drain_stream_checksum(&mut session, "SELECT ID FROM REPLAY").await;
        assert_eq!(count, rows, "block {block_size}");
        assert_eq!(sum, replay::checksum(rows), "block {block_size}");
        session.close().await.unwrap();
        server.abort();
    }
}

#[tokio::test]
async fn replay_partial_row_splits_at_odd_boundaries() {
    // 7-byte blocks split 6-byte INT rows mid-row on purpose.
    let rows = 100;
    let (port, server) = replay::spawn_replay_server(ReplayFixture::Int, rows, 7).await;
    let mut session = replay::connect_replay(port).await;
    let (count, sum) = replay::drain_stream_checksum(&mut session, "SELECT ID FROM REPLAY").await;
    assert_eq!(count, rows);
    assert_eq!(sum, replay::checksum(rows));
    session.close().await.unwrap();
    server.abort();
}
