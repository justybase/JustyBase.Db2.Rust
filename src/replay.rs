//! Deterministic DRDA replay fixtures for benchmarks and offline tests.
//!
//! This module is a synthetic client stress model, not a normative Db2 server.
//! It provides loopback-only TCP peers that speak the minimal DRDA subset used
//! by `Db2Session::query_stream` for single-shape `INTEGER` queries:
//! handshake (`EXCSAT`/`ACCSEC`/`SECCHK`/`ACCRDB`), open (`PRPSQLSTT`/`OPNQRY`),
//! fetch (`CNTQRY` → `QRYDTA`/`ENDQRYRM`), close (`CLSQRY`/`RDBCMM`).
//!
//! Fixture data is synthetic and deterministic. Row `n` (1-based) holds the
//! value `n` in every column; the checksum is `N * (N + 1) / 2` per column.
//! No captures, credentials, or customer data are stored.
//!
//! Tier: low-level unstable testing/tooling helper, not the primary
//! application API. The wire shape may change between alpha releases.

use crate::config::Config;
use crate::network::codepoint as cp;
use crate::network::ddm;
use crate::network::dss::{self, DssPacket};
use crate::network::session::Db2Session;

/// Named replay fixtures. All fixtures currently use `INTEGER` columns so the
/// client decode path stays deterministic; `Mixed`/`Wide` differ by column
/// count, matching the cs-db2 replay family shape (narrow vs wide).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReplayFixture {
    /// 1 `INTEGER` column.
    Int,
    /// 2 `INTEGER` columns.
    Mixed,
    /// 10 `INTEGER` columns.
    Wide,
    /// 2 `INTEGER` columns, used for early-terminate (`Read1`/`Read10`+close).
    Discard,
    /// 1 `INTEGER` column, exactly 1 row.
    Small,
}

impl ReplayFixture {
    /// Short name used in bench IDs and CSV output.
    pub fn name(self) -> &'static str {
        match self {
            Self::Int => "Int",
            Self::Mixed => "Mixed",
            Self::Wide => "Wide",
            Self::Discard => "Discard",
            Self::Small => "Small",
        }
    }

    /// Number of `INTEGER` columns in the fixture.
    pub fn columns(self) -> usize {
        match self {
            Self::Int => 1,
            Self::Mixed => 2,
            Self::Wide => 10,
            Self::Discard => 2,
            Self::Small => 1,
        }
    }

    /// Default row count for benchmarks. Deliberately smaller than the cs-db2
    /// `1M/100K/250K` corpus: loopback Criterion benches must finish in
    /// seconds, not minutes. Correctness tests use even smaller counts.
    pub fn bench_rows(self) -> usize {
        match self {
            Self::Int => 20_000,
            Self::Mixed => 10_000,
            Self::Wide => 2_000,
            Self::Discard => 10_000,
            Self::Small => 1,
        }
    }

    /// Full reference row counts mirroring the cs-db2 replay family. Used only
    /// for checksum documentation and the one-shot validation probe, not for
    /// timed Criterion iterations.
    pub fn reference_rows(self) -> usize {
        match self {
            Self::Int => 1_000_000,
            Self::Mixed => 100_000,
            Self::Wide => 100_000,
            Self::Discard => 250_000,
            Self::Small => 1,
        }
    }
}

/// Expected checksum: `sum(1..=n)` for the first column.
pub fn checksum(rows: usize) -> u64 {
    (rows as u64) * (rows as u64 + 1) / 2
}

/// QRYDSC payload for `columns` `INTEGER` columns.
/// Single-column form matches the existing session tests:
/// `[6, 0x76, 0xd0, 0x02, 0, 4]`.
pub fn qrydsc_integer(columns: usize) -> Vec<u8> {
    assert!((1..=84).contains(&columns), "unsupported column count");
    let mut out = Vec::with_capacity(3 + 3 * columns);
    out.push((3 + 3 * columns) as u8);
    out.push(0x76);
    out.push(0xd0);
    for _ in 0..columns {
        out.extend_from_slice(&[0x02, 0, 4]);
    }
    out
}

/// Encodes rows `start..=end` (1-based, inclusive) as QRYDTA payload bytes.
/// Each row is `[0xff, 0x00]` null indicator plus little-endian `i32` per
/// column, matching `bench_value_encoding` synthetic shapes.
/// An empty range (`start > end`, e.g. `start == end + 1`) encodes to an
/// empty byte stream.
pub fn encode_int_rows(columns: usize, start: usize, end: usize) -> Vec<u8> {
    assert!(start >= 1 && start <= end + 1);
    if start > end {
        return Vec::new();
    }
    let mut out = Vec::with_capacity((end - start + 1) * (2 + 4 * columns));
    for row in start..=end {
        out.push(0xff);
        out.push(0x00);
        for _ in 0..columns {
            out.extend_from_slice(&(row as i32).to_le_bytes());
        }
    }
    out
}

/// Splits a QRYDTA byte stream into `block_size` chunks. Splits deliberately
/// land inside rows to exercise partial-row handling; this is a client stress
/// model and does not prove Db2 emits these exact shapes.
pub fn split_blocks(data: &[u8], block_size: usize) -> Vec<Vec<u8>> {
    assert!(block_size > 0);
    data.chunks(block_size).map(|c| c.to_vec()).collect()
}

fn sql_card_ok() -> [u8; 54] {
    sql_card_with(0, "00000")
}

fn sql_card_with(code: i32, state: &str) -> [u8; 54] {
    let mut result = [0; 54];
    result[1..5].copy_from_slice(&code.to_le_bytes());
    result[5..10].copy_from_slice(state.as_bytes());
    result
}

async fn read_chain(socket: &mut tokio::net::TcpStream) -> Vec<DssPacket> {
    // Graceful variant: a client disconnect yields whatever was read so far
    // (possibly empty) instead of panicking the spawned peer task.
    read_chain_inner(socket).await
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
    // A disconnected client is a normal early-terminate outcome; never panic
    // the peer task on write failure.
    let _ = socket.write_all(&bytes).await;
}

async fn serve_handshake(socket: &mut tokio::net::TcpStream) -> bool {
    let chain = read_chain(socket).await;
    if chain.is_empty()
        || chain.iter().map(|p| p.codepoint).collect::<Vec<_>>() != vec![cp::EXCSAT, cp::ACCSEC]
    {
        return false;
    }
    send_reply(socket, cp::ACCSECRD, &ddm::pack_u16(cp::SECMEC, 3), false).await;
    let chain = read_chain(socket).await;
    if chain.is_empty()
        || chain.iter().map(|p| p.codepoint).collect::<Vec<_>>() != vec![cp::SECCHK, cp::ACCRDB]
    {
        return false;
    }
    send_reply(
        socket,
        cp::SECCHKRM,
        &ddm::pack_bytes(cp::SECCHKCD, &[0]),
        true,
    )
    .await;
    send_reply(socket, cp::ACCRDBRM, &[], false).await;
    // `initialize_session` + client-info setup send a variable number of
    // `EXCSQLSET`/`SQLSTT`/`RDBCMM` chains. Answer each with SQLCARD until the
    // first query open arrives; the query loop below takes over from there.
    // To avoid a deadlock we peek the next chain inside `serve_query_blocks`.
    true
}

/// Serves one query stream with precomputed QRYDTA blocks.
/// Tolerates: `OPNQRY` open, repeated `CNTQRY` fetch, `CLSQRY` early close,
/// terminal `RDBCMM`, and session `close()` (EOF).
pub async fn serve_query_blocks(
    socket: &mut tokio::net::TcpStream,
    qrydsc: Vec<u8>,
    blocks: Vec<Vec<u8>>,
) {
    let mut next = 0usize;
    let mut qryinsid = [0u8; 8];
    qryinsid.copy_from_slice(&1u64.to_be_bytes());
    // Unified dispatch loop: session-setup / client-info chains get SQLCARD,
    // the first OPNQRY opens the cursor, CNTQRY fetches blocks, CLSQRY and
    // RDBCMM close/commit, EOF ends the peer. All reads use the 30 s idle
    // timeout below so a stalled client ends the peer instead of hanging it.
    let mut pending: Option<Vec<DssPacket>> = None;
    loop {
        let chain = match pending.take() {
            Some(chain) => chain,
            None => match tokio::time::timeout(
                std::time::Duration::from_secs(30),
                read_chain_inner(socket),
            )
            .await
            {
                Ok(chain) => chain,
                Err(_) => return,
            },
        };
        if chain.is_empty() {
            return;
        }
        let kinds: Vec<cp::CodePoint> = chain.iter().map(|p| p.codepoint).collect();
        if kinds.contains(&cp::OPNQRY) {
            send_reply(socket, cp::QRYDSC, &qrydsc, true).await;
            send_reply(
                socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &qryinsid),
                true,
            )
            .await;
            if next < blocks.len() {
                let is_last_block = blocks.len() == 1;
                // QRYDTA is chained only when ENDQRYRM follows in the same
                // chain (single-block case). Multi-block first chunks end
                // their chain; the client fetches the rest via CNTQRY.
                send_reply(socket, cp::QRYDTA, &blocks[next], is_last_block).await;
                next += 1;
                if is_last_block {
                    send_reply(socket, cp::ENDQRYRM, &[], false).await;
                }
            } else {
                send_reply(socket, cp::ENDQRYRM, &[], false).await;
            }
            continue;
        }
        let first = chain[0].codepoint;
        if first == cp::CNTQRY {
            if next < blocks.len() {
                let is_last_block = next + 1 == blocks.len();
                send_reply(socket, cp::QRYDTA, &blocks[next], is_last_block).await;
                next += 1;
                if is_last_block {
                    send_reply(socket, cp::ENDQRYRM, &[], false).await;
                }
            } else {
                send_reply(socket, cp::ENDQRYRM, &[], false).await;
            }
        } else if first == cp::CLSQRY || first == cp::RDBCMM || first == cp::RDBRLLBCK {
            send_reply(socket, cp::SQLCARD, &sql_card_ok(), false).await;
        } else if kinds.contains(&cp::EXCSQLSET)
            || kinds.contains(&cp::SQLSTT)
            || kinds.contains(&cp::EXCSAT)
        {
            // Session setup / SET CLIENT / commit-setup chains.
            send_reply(socket, cp::SQLCARD, &sql_card_ok(), false).await;
        } else {
            // Unknown follow-up (e.g. next query on a reused connection):
            // answer conservatively so the client fails closed with a visible
            // mismatch rather than hanging.
            send_reply(socket, cp::SQLCARD, &sql_card_with(-30000, "58000"), false).await;
            return;
        }
    }
}

async fn read_chain_inner(socket: &mut tokio::net::TcpStream) -> Vec<DssPacket> {
    let mut result = Vec::new();
    loop {
        match dss::read_dss(socket).await {
            Ok(packet) => {
                let chained = packet.header.chained;
                result.push(packet);
                if !chained {
                    return result;
                }
            }
            Err(_) => return result,
        }
    }
}

/// Spawns a loopback replay server for one connection serving the given
/// fixture rows split at `block_size`. Returns the TCP port.
pub async fn spawn_replay_server(
    fixture: ReplayFixture,
    rows: usize,
    block_size: usize,
) -> (u16, tokio::task::JoinHandle<()>) {
    let qrydsc = qrydsc_integer(fixture.columns());
    let data = if rows == 0 {
        Vec::new()
    } else {
        encode_int_rows(fixture.columns(), 1, rows)
    };
    let blocks = if data.is_empty() {
        Vec::new()
    } else {
        split_blocks(&data, block_size)
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("replay listener bind");
    let port = listener.local_addr().expect("replay addr").port();
    let server = tokio::spawn(async move {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        if !serve_handshake(&mut socket).await {
            return;
        }
        serve_query_blocks(&mut socket, qrydsc, blocks).await;
    });
    (port, server)
}

/// Connects a `Db2Session` to a loopback replay peer. No credentials leave the
/// process; the DSN password is a fixed test value.
pub async fn connect_replay(port: u16) -> Db2Session {
    let config = Config {
        host: "127.0.0.1".into(),
        port,
        database: "TESTDB".into(),
        user: "db2inst1".into(),
        password: "password".into(),
        security_mechanism: 3,
        timeout_ms: 10_000,
        ..Default::default()
    };
    Db2Session::connect(config).await.expect("replay connect")
}

/// Reads all rows from a replay stream and returns `(row_count, checksum)` of
/// the first column. Checksum is computed outside timing in benches.
pub async fn drain_stream_checksum(session: &mut Db2Session, sql: &str) -> (usize, u64) {
    let mut reader = session.query_stream(sql).await.expect("replay open");
    let mut count = 0usize;
    let mut sum = 0u64;
    while let Some(row) = reader.next().await.expect("replay next") {
        count += 1;
        if let Some(crate::Db2Value::Integer(v)) = row.first() {
            sum += *v as u64;
        }
    }
    reader.close().await.expect("replay close");
    (count, sum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_checksums_match_closed_form() {
        assert_eq!(checksum(1), 1);
        assert_eq!(checksum(100), 5050);
        assert_eq!(checksum(1_000_000), 500000500000);
    }

    #[test]
    fn int_row_encoding_matches_decoder_shape() {
        let bytes = encode_int_rows(1, 1, 2);
        assert_eq!(bytes, vec![0xff, 0, 1, 0, 0, 0, 0xff, 0, 2, 0, 0, 0]);
        let two_col = encode_int_rows(2, 1, 1);
        assert_eq!(two_col, vec![0xff, 0, 1, 0, 0, 0, 1, 0, 0, 0]);
    }

    #[test]
    fn empty_row_range_encodes_to_empty() {
        // `start == end + 1` is the documented empty range; it must not
        // underflow the capacity calculation.
        assert!(encode_int_rows(1, 1, 0).is_empty());
        assert!(encode_int_rows(2, 5, 4).is_empty());
    }

    #[test]
    fn block_split_covers_partial_rows() {
        let data = encode_int_rows(1, 1, 10);
        let blocks = split_blocks(&data, 7);
        let rejoined: Vec<u8> = blocks.concat();
        assert_eq!(rejoined, data);
        assert!(blocks.len() > 1);
    }

    #[test]
    fn qrydsc_single_column_matches_session_tests() {
        assert_eq!(qrydsc_integer(1), vec![6, 0x76, 0xd0, 0x02, 0, 4]);
    }
}
