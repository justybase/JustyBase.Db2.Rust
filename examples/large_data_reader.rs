use justybase_db2::{Db2Session, Db2Value};
use ring::digest::{Context, SHA256};
use std::fs;

const PAYLOAD_BYTES: u64 = 1 << 30;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2
        || !valid_table(&args[0])
        || args[1].len() != 64
        || !args[1].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err("Usage: large_data_reader LD_<12HEX> <sha256>".into());
    }

    let dsn = std::env::var("DB2_TEST_DSN")?;
    let mut session = Db2Session::connect_dsn(&dsn).await?;
    let mut rows = session
        .query_stream(&format!("SELECT PAYLOAD FROM {} WHERE ID = 1", args[0]))
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or("Large LOB fixture row is missing")?;
    let lob = match row.first() {
        Some(Db2Value::Lob(value)) => *value,
        _ => return Err("Db2 did not expose the 1 GiB value as a progressive LOB stream".into()),
    };
    if lob.byte_length != PAYLOAD_BYTES {
        return Err(format!("LOB length mismatch: {}", lob.byte_length).into());
    }

    let mut reader = rows.open_lob(&lob).await?;
    let mut count = 0u64;
    let mut digest = Context::new(&SHA256);
    while let Some(chunk) = reader.next_chunk().await? {
        count += chunk.len() as u64;
        digest.update(&chunk);
    }
    let _ = reader;
    rows.close().await?;
    let actual = digest.finish();
    let actual = actual
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if count != PAYLOAD_BYTES || actual.to_lowercase() != args[1].to_lowercase() {
        return Err(
            format!("Stream verification failed: read {count} bytes, SHA-256 {actual}").into(),
        );
    }
    let rss = peak_rss_bytes()?;
    println!(
        "{{\"type\":\"large-data-evidence\",\"language\":\"rust\",\"bytes\":{count},\"checksumMatched\":true,\"expectedSha256\":\"{}\",\"actualSha256\":\"{}\",\"mode\":\"stream\",\"maxRssBytes\":{rss},\"maxSpoolBytes\":0}}",
        args[1].to_lowercase(),
        actual.to_lowercase()
    );
    session.close().await?;
    Ok(())
}

fn valid_table(table: &str) -> bool {
    let Some(suffix) = table.strip_prefix("LD_") else {
        return false;
    };
    suffix.len() == 12 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn peak_rss_bytes() -> Result<u64, std::io::Error> {
    let status = fs::read_to_string("/proc/self/status")?;
    let value = status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|line| line.split_whitespace().next())
        })
        .ok_or_else(|| std::io::Error::other("/proc/self/status lacks VmHWM"))?;
    Ok(value.parse::<u64>().map_err(std::io::Error::other)? * 1024)
}
