//! Single-case replay probe: exact row count, checksum, peer completion.
//!
//! Usage: `cargo run --example replay_probe -- Int 20000 65535`
//! Prints `fixture rows block_size count checksum` for manual inspection and
//! allocation-trace entry points. Peer work runs in another task.
use justybase_db2::replay::{self, ReplayFixture};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let fixture = match args.get(1).map(String::as_str) {
        Some("Mixed") => ReplayFixture::Mixed,
        Some("Wide") => ReplayFixture::Wide,
        Some("Discard") => ReplayFixture::Discard,
        Some("Small") => ReplayFixture::Small,
        _ => ReplayFixture::Int,
    };
    let rows: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(10_000);
    let block: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(65535);
    let (port, server) = replay::spawn_replay_server(fixture, rows, block).await;
    let mut session = replay::connect_replay(port).await;
    let (count, sum) = replay::drain_stream_checksum(&mut session, "SELECT ID FROM REPLAY").await;
    session.close().await.unwrap();
    server.abort();
    println!(
        "fixture={} rows={rows} block_size={block} count={count} checksum={sum} expected={}",
        fixture.name(),
        replay::checksum(rows)
    );
    assert_eq!(count, rows);
    assert_eq!(sum, replay::checksum(rows));
}
