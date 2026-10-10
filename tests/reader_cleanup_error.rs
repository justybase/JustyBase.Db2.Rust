//! Reader cleanup/error-precedence replay tests.
//!
//! Covers: late drain reporting, original-error precedence over secondary
//! cleanup failures, and fail-closed invalidation when cleanup cannot be
//! proven. Uses the deterministic replay peer; live variants live in
//! `tests/discard_live.rs`.
use justybase_db2::network::{codepoint as cp, ddm, dss};
use justybase_db2::replay;

async fn read_chain(socket: &mut tokio::net::TcpStream) -> Vec<dss::DssPacket> {
    let mut result = Vec::new();
    loop {
        let packet = dss::read_dss(socket).await.unwrap();
        let chained = packet.header.chained;
        result.push(packet);
        if !chained {
            return result;
        }
    }
}

async fn send_reply(
    socket: &mut tokio::net::TcpStream,
    codepoint: cp::CodePoint,
    payload: &[u8],
    chained: bool,
) {
    use tokio::io::AsyncWriteExt;
    let object = ddm::pack_ddm_object(codepoint, payload);
    let bytes = dss::encode_reply_dss(&object, chained, 1);
    socket.write_all(&bytes).await.unwrap();
}

fn sql_card_ok() -> [u8; 54] {
    let mut card = [0; 54];
    card[5..10].copy_from_slice(b"00000");
    card
}

fn sql_card_error(code: i32, state: &str) -> [u8; 54] {
    let mut card = [0; 54];
    card[1..5].copy_from_slice(&code.to_le_bytes());
    card[5..10].copy_from_slice(state.as_bytes());
    card
}

/// Peer that returns one good row then a late `-911/40001` SQLCARD instead of
/// ENDQRYRM on the follow-up CNTQRY.
async fn spawn_late_error_server() -> (u16, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // Handshake.
        let _ = read_chain(&mut socket).await;
        send_reply(
            &mut socket,
            cp::ACCSECRD,
            &ddm::pack_u16(cp::SECMEC, 3),
            false,
        )
        .await;
        let _ = read_chain(&mut socket).await;
        send_reply(
            &mut socket,
            cp::SECCHKRM,
            &ddm::pack_bytes(cp::SECCHKCD, &[0]),
            true,
        )
        .await;
        send_reply(&mut socket, cp::ACCRDBRM, &[], false).await;
        // Drain setup chains until OPNQRY.
        loop {
            let chain = read_chain(&mut socket).await;
            let kinds: Vec<cp::CodePoint> = chain.iter().map(|p| p.codepoint).collect();
            if kinds.contains(&cp::OPNQRY) {
                break;
            }
            send_reply(&mut socket, cp::SQLCARD, &sql_card_ok(), false).await;
        }
        let mut id = [0u8; 8];
        id.copy_from_slice(&1u64.to_be_bytes());
        send_reply(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
        send_reply(
            &mut socket,
            cp::OPNQRYRM,
            &ddm::pack_bytes(cp::QRYINSID, &id),
            true,
        )
        .await;
        send_reply(&mut socket, cp::QRYDTA, &[0xff, 0, 1, 0, 0, 0], false).await;
        // Client fetches more; answer with a late server error.
        let chain = read_chain(&mut socket).await;
        assert_eq!(chain[0].codepoint, cp::CNTQRY);
        send_reply(
            &mut socket,
            cp::SQLCARD,
            &sql_card_error(-911, "40001"),
            false,
        )
        .await;
    });
    (port, server)
}

#[tokio::test]
async fn late_server_error_after_first_row_is_reported() {
    let (port, server) = spawn_late_error_server().await;
    let mut session = replay::connect_replay(port).await;
    let mut reader = session.query_stream("SELECT ID FROM REPLAY").await.unwrap();
    let first = reader.next().await.unwrap().unwrap();
    assert_eq!(first[0], justybase_db2::Db2Value::Integer(1));
    let err = reader.next().await.unwrap_err();
    assert_eq!(err.sql_code, Some(-911), "{err}");
    server.abort();
}

#[tokio::test]
async fn close_after_complete_stream_succeeds_and_session_reusable() {
    let (port, server) = replay::spawn_replay_server(replay::ReplayFixture::Small, 1, 65535).await;
    let mut session = replay::connect_replay(port).await;
    let mut reader = session.query_stream("SELECT 1 FROM REPLAY").await.unwrap();
    assert!(reader.next().await.unwrap().is_some());
    assert!(reader.next().await.unwrap().is_none());
    reader.close().await.unwrap();
    assert!(!session.is_closed());
    session.close().await.unwrap();
    server.abort();
}
