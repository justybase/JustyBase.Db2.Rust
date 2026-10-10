use std::{error::Error, fs};

use justybase_db2::{
    Db2ImportColumn, Db2ImportPolicy, Db2ImportTarget, Db2LobEncoding, Db2Parameter,
    Db2ParameterDirection, Db2Pool, Db2Session, Db2Value,
};

const WARMUP: usize = 10;
const CYCLES: usize = 100;

fn identifier(value: &str, suffix: &str) -> bool {
    value.len() == 15 + suffix.len()
        && value.starts_with("SO_")
        && value[3..15]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_lowercase())
        && value.as_bytes().get(15) == Some(&b'_')
        && value.ends_with(suffix)
}

fn runtime_sample(
    phase: &str,
    cycle: usize,
    pool: &Db2Pool,
) -> Result<(usize, Vec<String>), Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    let rss_kib: usize = status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse().ok())
        })
        .ok_or("missing VmRSS in /proc/self/status")?;
    let rss = rss_kib * 1024;
    if rss > 512 * 1024 * 1024 {
        return Err("RSS exceeds the 512 MiB qualification budget".into());
    }
    let fds = fs::read_dir("/proc/self/fd")?.count();
    let temp = std::env::temp_dir();
    // tempfile::NamedTempFile::new uses `.tmp` plus six random characters. This is a
    // process-global directory listing; entries are not attributed to this driver.
    let mut temp_files = fs::read_dir(temp)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.len() == 10
                && name.starts_with(".tmp")
                && name[4..].bytes().all(|b| b.is_ascii_alphanumeric())
        })
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    temp_files.sort();
    let file_json = temp_files
        .iter()
        .map(|name| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "{{\"language\":\"rust\",\"phase\":\"{phase}\",\"cycle\":{cycle},\"rssBytes\":{rss},\"fdCount\":{fds},\"tempfileEntries\":[{file_json}],\"poolSessions\":{},\"poolIdle\":{},\"warmupCycles\":{WARMUP},\"sampleIntervalCycles\":10}}",
        pool.session_count(),
        pool.idle_session_count()
    );
    Ok((fds, temp_files))
}

async fn materialized(
    session: &mut Db2Session,
    table: &str,
) -> Result<Vec<Vec<Db2Value>>, Box<dyn Error>> {
    let mut rows = session
        .query_stream(&format!(
            "SELECT ID, B, C, G, X FROM \"{table}\" ORDER BY ID"
        ))
        .await?;
    let mut output = Vec::new();
    while let Some(row) = rows.next().await? {
        let mut normalized = Vec::with_capacity(row.len());
        for value in &row {
            if let Db2Value::Lob(locator) = value {
                let encoding = locator.encoding;
                let mut lob = rows.open_lob(locator).await?;
                let mut bytes = Vec::new();
                while let Some(chunk) = lob.next_chunk().await? {
                    bytes.extend(chunk);
                }
                let _ = lob;
                let value = match encoding {
                    Db2LobEncoding::Binary => Db2Value::Bytes(bytes),
                    Db2LobEncoding::Utf8 => Db2Value::Text(String::from_utf8(bytes)?),
                    Db2LobEncoding::Utf16Be => {
                        let words = bytes
                            .as_chunks::<2>()
                            .0
                            .iter()
                            .map(|p| u16::from_be_bytes([p[0], p[1]]))
                            .collect::<Vec<_>>();
                        Db2Value::Text(String::from_utf16(&words)?)
                    }
                };
                normalized.push(value);
            } else {
                normalized.push(value.clone());
            }
        }
        output.push(normalized);
    }
    rows.close().await?;
    Ok(output)
}

fn verify_x(rows: &[Vec<Db2Value>]) -> Result<(), Box<dyn Error>> {
    if rows.len() != 3 || rows.iter().any(|row| row.len() != 5) {
        return Err("X fixture row shape mismatch".into());
    }
    for (index, row) in rows.iter().enumerate() {
        if row[0] != Db2Value::Integer(index as i64 + 1) {
            return Err("X fixture IDs mismatch".into());
        }
    }
    let Db2Value::Bytes(blob) = &rows[0][1] else {
        return Err("X BLOB was not materialized".into());
    };
    if blob.len() != 2_500_123
        || blob
            .iter()
            .enumerate()
            .any(|(i, b)| *b != ((i * 37 + 11) % 256) as u8)
    {
        return Err("X BLOB pattern or length mismatch".into());
    }
    if rows[1][1] != Db2Value::Bytes(Vec::new()) || rows[2][1] != Db2Value::Null {
        return Err("X BLOB empty/NULL shape mismatch".into());
    }
    for column in [2, 3] {
        if rows[0][column] == Db2Value::Null
            || rows[1][column] != Db2Value::Text(String::new())
            || rows[2][column] != Db2Value::Null
        {
            return Err("X character LOB empty/NULL shape mismatch".into());
        }
    }
    if rows[0][4] == Db2Value::Null
        || rows[1][4] != Db2Value::Text("<empty/>".into())
        || rows[2][4] != Db2Value::Null
    {
        return Err("X XML empty/NULL shape mismatch".into());
    }
    Ok(())
}

async fn check_n(session: &mut Db2Session, table: &str) -> Result<(), Box<dyn Error>> {
    let mut rows = session
        .query_stream(&format!("SELECT ID, V FROM \"{table}\" ORDER BY ID"))
        .await?;
    let first = rows.next().await?.ok_or("N fixture is empty")?;
    if first.len() != 2
        || first[0] != Db2Value::Integer(1)
        || first[1] != Db2Value::Text("Zażółć 東京 1  ".into())
    {
        return Err("N fixture first row mismatch".into());
    }
    let retained = first;
    for id in 2..=1000 {
        let row = rows.next().await?.ok_or("N fixture ended early")?;
        if row.len() != 2
            || row[0] != Db2Value::Integer(id)
            || row[1] != Db2Value::Text(format!("Zażółć 東京 {id}  "))
        {
            return Err(format!("N fixture row {id} mismatch").into());
        }
    }
    if rows.next().await?.is_some() {
        return Err("N fixture has extra rows".into());
    }
    if retained
        != [
            Db2Value::Integer(1),
            Db2Value::Text("Zażółć 東京 1  ".into()),
        ]
    {
        return Err("retained first row changed while advancing the query".into());
    }
    rows.close().await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cycle(
    source_session: &mut Db2Session,
    destination: &mut Db2Session,
    source: &str,
    dest: &str,
    source_x: &str,
    dest_x: &str,
    procedure: &str,
    pool: &Db2Pool,
    i: usize,
) -> Result<(), Box<dyn Error>> {
    check_n(source_session, source).await?;
    let mut held = source_session
        .query_stream(&format!("SELECT ID FROM \"{source}\" ORDER BY ID"))
        .await?;
    let _ = held.next().await?;
    held.close().await?;
    if source_session.query("VALUES 41").await?.rows != [vec![Db2Value::Integer(41)]] {
        return Err("session reuse after partial query failed".into());
    }

    let mut lease = pool.acquire().await?;
    let mut pooled = lease
        .query_stream(&format!("SELECT ID FROM \"{source}\" ORDER BY ID"))
        .await?;
    let _ = pooled.next().await?;
    pooled.close().await?;
    lease.release().await?;
    if pool.session_count() != 1 || pool.idle_session_count() != 1 {
        return Err("pool did not return its one lease to idle".into());
    }

    let target = Db2ImportTarget::new(
        None,
        dest,
        vec![
            Db2ImportColumn::new("ID").with_sql_type(497),
            Db2ImportColumn::new("V"),
        ],
    )?;
    let max_rows = [127, 128, 129][i % 3];
    for atomic in [true, false] {
        destination
            .execute(&format!("DELETE FROM \"{dest}\""))
            .await?;
        let mut source_rows = source_session
            .query_stream(&format!("SELECT ID, V FROM \"{source}\" ORDER BY ID"))
            .await?;
        let policy = if atomic {
            Db2ImportPolicy::Atomic {
                max_batch_rows: max_rows,
                max_batch_bytes: 65_536,
            }
        } else {
            Db2ImportPolicy::CommitEveryRows {
                rows: max_rows,
                max_batch_bytes: 65_536,
            }
        };
        let progress = destination
            .import_rows_from(&mut source_rows, &target, policy, |_| Ok(()))
            .await?;
        source_rows.close().await?;
        let expected_batches = if atomic {
            1
        } else {
            1000_usize.div_ceil(max_rows)
        };
        if progress.source_rows_read != 1000
            || progress.committed_rows != 1000
            || progress.committed_batches != expected_batches as u64
        {
            return Err("N import progress mismatch".into());
        }
        check_n(destination, dest).await?;
    }

    if i.is_multiple_of(10) {
        let mut partial = source_session
            .query_stream(&format!("SELECT B FROM \"{source_x}\" WHERE ID = 1"))
            .await?;
        let row = partial.next().await?.ok_or("X BLOB source row missing")?;
        if let Db2Value::Lob(locator) = &row[0] {
            let mut lob = partial.open_lob(locator).await?;
            let _ = lob.next_chunk().await?;
        }
        partial.close().await?;
        let lob_target = Db2ImportTarget::new(
            None,
            dest_x,
            ["ID", "B", "C", "G", "X"]
                .into_iter()
                .map(Db2ImportColumn::new)
                .collect(),
        )?;
        let source_x_rows = materialized(source_session, source_x).await?;
        verify_x(&source_x_rows)?;
        for atomic in [true, false] {
            destination
                .execute(&format!("DELETE FROM \"{dest_x}\""))
                .await?;
            let mut lob_rows = source_session
                .query_stream(&format!(
                    "SELECT ID, B, C, G, X FROM \"{source_x}\" ORDER BY ID"
                ))
                .await?;
            let lob_policy = if atomic {
                Db2ImportPolicy::Atomic {
                    max_batch_rows: 2,
                    max_batch_bytes: 65_536,
                }
            } else {
                Db2ImportPolicy::CommitEveryRows {
                    rows: 2,
                    max_batch_bytes: 65_536,
                }
            };
            let lob_progress = destination
                .import_rows_from(&mut lob_rows, &lob_target, lob_policy, |_| Ok(()))
                .await?;
            lob_rows.close().await?;
            // Progressive LOB rows can flush independently before the cursor
            // advances. Row and byte caps are maxima, not fixed commit sizes.
            let valid_batches = if atomic {
                lob_progress.committed_batches == 1
            } else {
                (2..=3).contains(&lob_progress.committed_batches)
            };
            if lob_progress.source_rows_read != 3
                || lob_progress.committed_rows != 3
                || !valid_batches
            {
                return Err(format!(
                    "X fixture import progress mismatch: atomic={atomic}, {lob_progress:?}"
                )
                .into());
            }
            let dest_x_rows = materialized(destination, dest_x).await?;
            if source_x_rows != dest_x_rows {
                return Err("X destination differs from source after import".into());
            }
        }
        let statement = source_session
            .prepare(&format!("CALL {procedure}(?)"))
            .await?;
        let mut procedure_reader = source_session
            .call_procedure_stream(
                &statement,
                &[Db2Parameter {
                    value: Db2Value::Integer(0),
                    direction: Db2ParameterDirection::Output,
                }],
            )
            .await?;
        let mut sets = Vec::new();
        loop {
            let mut rows = Vec::new();
            while let Some(row) = procedure_reader.next().await? {
                rows.push(row);
            }
            sets.push(rows);
            if !procedure_reader.next_result_set().await? {
                break;
            }
        }
        let terminal = procedure_reader.finish().await?;
        if sets
            != vec![
                vec![vec![Db2Value::Integer(17)]],
                Vec::new(),
                vec![vec![Db2Value::Integer(19)]],
            ]
            || terminal.output_parameters.last() != Some(&Db2Value::Integer(23))
        {
            return Err("procedure result sets or terminal OUT parameter mismatch".into());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 5
        || !(identifier(&args[0], "_N") || identifier(&args[0], "_ND"))
        || !(identifier(&args[1], "_N") || identifier(&args[1], "_ND"))
        || !(identifier(&args[2], "_X") || identifier(&args[2], "_XD"))
        || !(identifier(&args[3], "_X") || identifier(&args[3], "_XD"))
        || !identifier(&args[4], "_P")
    {
        return Err("usage: resource_soak SOURCE_N DEST_N SOURCE_X DEST_X PROCEDURE_P (SO_<12 uppercase hex>_N/ND/X/XD/P identifiers required)".into());
    }
    let dsn = std::env::var("DB2_TEST_DSN")?;
    let pool = Db2Pool::connect_dsn(&dsn, 1)?;
    let (initial_fds, initial_tempfiles) = runtime_sample("startup", 0, &pool)?;
    let mut source_session = Db2Session::connect_dsn(&dsn).await?;
    let mut destination = Db2Session::connect_dsn(&dsn).await?;
    for i in 0..WARMUP {
        cycle(
            &mut source_session,
            &mut destination,
            &args[0],
            &args[1],
            &args[2],
            &args[3],
            &args[4],
            &pool,
            i,
        )
        .await?;
    }
    // Baseline includes the established idle pool session and all post-warmup runtime state.
    let (baseline_fds, baseline_tempfiles) = runtime_sample("post-warmup", 0, &pool)?;
    let duration_seconds = std::env::var("DB2_SOAK_DURATION_SECONDS")
        .unwrap_or_else(|_| "0".into())
        .parse::<u64>()?;
    if duration_seconds > 3600 {
        return Err("Invalid soak duration (0..3600 seconds)".into());
    }
    let measured_start = std::time::Instant::now();
    let mut measured_cycles = 0;
    while measured_cycles < CYCLES || measured_start.elapsed().as_secs() < duration_seconds {
        let i = measured_cycles;
        cycle(
            &mut source_session,
            &mut destination,
            &args[0],
            &args[1],
            &args[2],
            &args[3],
            &args[4],
            &pool,
            i + WARMUP,
        )
        .await?;
        measured_cycles += 1;
        if (i + 1) % 10 == 0 {
            let (fds, files) = runtime_sample("measured", i + 1, &pool)?;
            if fds > baseline_fds || files != baseline_tempfiles {
                return Err("FD or tempfile entries grew after warmup".into());
            }
        }
    }
    println!(
        "{{\"type\":\"resource-duration\",\"language\":\"rust\",\"elapsedMs\":{},\"measuredCycles\":{measured_cycles}}}",
        measured_start.elapsed().as_millis()
    );
    destination.close().await?;
    source_session.close().await?;
    // Drop the closed owners too: close() shuts down the transport while the
    // Rust object still owns its OS descriptor until it is dropped.
    drop(destination);
    drop(source_session);
    pool.close().await?;
    let (final_fds, final_tempfiles) = runtime_sample("final-closed", measured_cycles, &pool)?;
    if final_fds != initial_fds || final_tempfiles != initial_tempfiles {
        return Err(
            "runtime FD count or global tempfile entry set did not return to its warmup baseline"
                .into(),
        );
    }
    Ok(())
}
