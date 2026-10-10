//! P1 paired fetch benchmark, native Rust driver. Emits JSON lines matching the
//! C#/TypeScript harnesses for the shared orchestrator.
//!
//! Usage: DB2_TEST_DSN=... cargo run --release --example p1_fetch -- \
//!   --dataset NARROW_10K [--modes native-materialized,native-stream] [--warmups 3] [--samples 7]
use justybase_db2::{Db2LobEncoding, Db2RowReader, Db2Session, Db2Value};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

struct CountingAllocator;
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

/// Per-mode benchmark samples: `(elapsed_ms, first_row_ms, rows, bytes)`.
type ModeSamples = std::collections::HashMap<String, Vec<(f64, Option<f64>, usize, usize)>>;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATED.fetch_add(new_size, Ordering::Relaxed);
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

struct Dataset {
    name: &'static str,
    select: &'static str,
    rows: u64,
    columns: &'static [&'static str],
    expected: &'static str,
}

const DATASETS: &[Dataset] = &[
    Dataset {
        name: "NARROW_10K",
        select: "SELECT ID, A, B FROM P1BENCH.NARROW_10K",
        rows: 10_000,
        columns: &["integer", "integer", "varchar"],
        expected: "40bfa5bdba75978c7c49b06e784eb16515cce61dcf6bb9357a0783d75b16218f",
    },
    Dataset {
        name: "NARROW_100K",
        select: "SELECT ID, A, B FROM P1BENCH.NARROW_100K",
        rows: 100_000,
        columns: &["integer", "integer", "varchar"],
        expected: "1b604b64c7c075981b94eac264e1162a0ef8caeb0195df9352a3a3fe26b89981",
    },
    Dataset {
        name: "NARROW_500K",
        select: "SELECT ID, A, B FROM P1BENCH.NARROW_500K",
        rows: 500_000,
        columns: &["integer", "integer", "varchar"],
        expected: "3a41ddebc5f7a7e3e0572e49503800aedfa0a9812f6adc859bba1f95f45d4ccc",
    },
    Dataset {
        name: "NARROW_1M",
        select: "SELECT ID, A, B FROM P1BENCH.NARROW_1M",
        rows: 1_000_000,
        columns: &["integer", "integer", "varchar"],
        expected: "f4535a4a64fa5110bdba7923e5d24f291e47eb1f73803b09515db760e0fea476",
    },
    Dataset {
        name: "INT_1M",
        select: "SELECT ID FROM P1BENCH.NARROW_1M",
        rows: 1_000_000,
        columns: &["integer"],
        expected: "4a8bfb7928605311654f2fc1743c365dc6636ced05005e2ee888a0b209124c68",
    },
    Dataset {
        name: "NARROW_5M",
        select: "SELECT ID, A, B FROM P1BENCH.NARROW_5M",
        rows: 5_000_000,
        columns: &["integer", "integer", "varchar"],
        expected: "4d1d779485a58432a5200cd54ac98c026d2d742dbe08cea408833507e7cab7d7",
    },
    Dataset {
        name: "MIXED_10K",
        select: "SELECT ID, A, C, D, E, F, G, H, I FROM P1BENCH.MIXED_10K",
        rows: 10_000,
        columns: &[
            "integer",
            "integer",
            "integer",
            "decimal",
            "double",
            "varchar",
            "date",
            "timestamp",
            "boolean",
        ],
        expected: "f5ffbd6f466aebcf32b63d8601ac11cdbfb3c5fede013a3ee95afc0804f396d0",
    },
    Dataset {
        name: "MIXED_100K",
        select: "SELECT ID, A, C, D, E, F, G, H, I FROM P1BENCH.MIXED_100K",
        rows: 100_000,
        columns: &[
            "integer",
            "integer",
            "integer",
            "decimal",
            "double",
            "varchar",
            "date",
            "timestamp",
            "boolean",
        ],
        expected: "96e55c93881278f102c992710d2ddebaa0b5f2f4f1197882140967a48feddfe1",
    },
    Dataset {
        name: "MIXED_1M",
        select: "SELECT ID, A, C, D, E, F, G, H, I FROM P1BENCH.MIXED_1M",
        rows: 1_000_000,
        columns: &[
            "integer",
            "integer",
            "integer",
            "decimal",
            "double",
            "varchar",
            "date",
            "timestamp",
            "boolean",
        ],
        expected: "898a5bd3b8c248d11e1b63da387f15001c20e1491c8892ec00eb7ff9fbc075d4",
    },
    Dataset {
        name: "SELECT_STAR_MIXED_1M",
        select: "SELECT * FROM P1BENCH.MIXED_1M",
        rows: 1_000_000,
        columns: &[
            "integer",
            "integer",
            "integer",
            "decimal",
            "double",
            "varchar",
            "date",
            "timestamp",
            "boolean",
        ],
        expected: "898a5bd3b8c248d11e1b63da387f15001c20e1491c8892ec00eb7ff9fbc075d4",
    },
    Dataset {
        name: "LOB_4K",
        select: "SELECT ID, B, C FROM P1BENCH.LOB_4K",
        rows: 1_000,
        columns: &["integer", "binary", "text"],
        expected: "65716aa2a6dbde71954d01995d735cdb5d5a6974305daa5a446f566c9b3b20c3",
    },
    Dataset {
        name: "LOB_32K",
        select: "SELECT ID, B, C FROM P1BENCH.LOB_32K",
        rows: 100,
        columns: &["integer", "binary", "text"],
        expected: "666b1a3115bd496c675f9d9c624c7ff18fa7b0b8e75d857963136939e2370b6c",
    },
    Dataset {
        name: "LOB_256K",
        select: "SELECT ID, B, C FROM P1BENCH.LOB_256K",
        rows: 100,
        columns: &["integer", "binary", "text"],
        expected: "70a7c77ace5713d0196a5ed5a8b2f3cde10349d7133ac0c4bf5f6299d0b93e28",
    },
    Dataset {
        name: "LOB_1M",
        select: "SELECT ID, B, C FROM P1BENCH.LOB_1M",
        rows: 10,
        columns: &["integer", "binary", "text"],
        expected: "1162cdf84366b3efc79b4c71d6a653b4dc94fb8d8c6cb93f9948295ecbadbcf0",
    },
    Dataset {
        name: "LOB_8M",
        select: "SELECT ID, B, C FROM P1BENCH.LOB_8M",
        rows: 1,
        columns: &["integer", "binary", "text"],
        expected: "62aa3e592984e21a23660a56b31e67b182809abf3da6adb7935fdfa57a093005",
    },
];

#[derive(Default)]
struct CanonicalHash {
    sums: [u64; 4],
    count: u64,
    bytes: u64,
}

impl CanonicalHash {
    fn add(&mut self, columns: &[&str], row: &[Db2Value]) {
        let mut buffer = Vec::new();
        for (index, column) in columns.iter().enumerate() {
            encode_value(
                &mut buffer,
                column,
                (index < row.len()).then(|| &row[index]),
            );
        }
        self.bytes += buffer.len() as u64;
        let digest = ring::digest::digest(&ring::digest::SHA256, &buffer);
        for word in 0..4 {
            let value =
                u64::from_le_bytes(digest.as_ref()[word * 8..word * 8 + 8].try_into().unwrap());
            self.sums[word] = self.sums[word].wrapping_add(value);
        }
        self.count += 1;
    }

    fn finish(&self) -> String {
        let mut input = [0u8; 40];
        input[..8].copy_from_slice(&self.count.to_be_bytes());
        for word in 0..4 {
            input[8 + word * 8..16 + word * 8].copy_from_slice(&self.sums[word].to_be_bytes());
        }
        hex(ring::digest::digest(&ring::digest::SHA256, &input).as_ref())
    }
}

fn encode_value(buffer: &mut Vec<u8>, column: &str, value: Option<&Db2Value>) {
    let value = value.unwrap_or(&Db2Value::Null);
    match (column, value) {
        (_, Db2Value::Null) => buffer.push(0x00),
        ("integer", Db2Value::Integer(number)) => {
            buffer.push(0x02);
            buffer.extend_from_slice(&number.to_be_bytes());
        }
        ("decimal", _) => {
            buffer.push(0x03);
            append_text(buffer, &text_of(value));
        }
        ("double", Db2Value::Float(number)) => {
            buffer.push(0x04);
            buffer.extend_from_slice(&number.to_be_bytes());
        }
        ("varchar" | "text", _) => {
            buffer.push(0x01);
            append_text(buffer, &text_of(value));
        }
        ("date" | "timestamp", _) => {
            buffer.push(0x05);
            append_text(buffer, &text_of(value));
        }
        ("boolean", Db2Value::Boolean(flag)) => buffer.extend_from_slice(&[0x06, u8::from(*flag)]),
        ("binary", Db2Value::Bytes(bytes)) => {
            buffer.push(0x07);
            append_length(buffer, bytes.len());
            buffer.extend_from_slice(bytes);
        }
        (_, other) => panic!("cannot canonically encode {other:?} for column {column}"),
    }
}

fn text_of(value: &Db2Value) -> String {
    match value {
        Db2Value::Text(text) => text.clone(),
        Db2Value::Decimal(text) => text.clone(),
        other => format!("{other:?}"),
    }
}

fn append_text(buffer: &mut Vec<u8>, text: &str) {
    append_length(buffer, text.len());
    buffer.extend_from_slice(text.as_bytes());
}

fn append_length(buffer: &mut Vec<u8>, length: usize) {
    buffer.extend_from_slice(&(length as u32).to_be_bytes());
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn peak_rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmHWM:"))?
                .split_whitespace()
                .nth(1)
                .map(|kb| kb.parse::<u64>().unwrap_or(0) * 1024)
        })
        .unwrap_or(0)
}

fn json(payload: &str) {
    println!("{payload}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mut dataset_name = None;
    let mut modes: Vec<String> = vec!["native-materialized".into(), "native-stream".into()];
    let mut warmups = 3usize;
    let mut samples = 7usize;
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--dataset" => {
                dataset_name = Some(args[index + 1].clone());
                index += 2;
            }
            "--modes" => {
                modes = args[index + 1].split(',').map(str::to_string).collect();
                index += 2;
            }
            "--warmups" => {
                warmups = args[index + 1].parse()?;
                index += 2;
            }
            "--samples" => {
                samples = args[index + 1].parse()?;
                index += 2;
            }
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    let dataset = DATASETS
        .iter()
        .find(|d| Some(d.name.to_string()) == dataset_name)
        .ok_or("--dataset is required and must exist")?;
    let dsn = std::env::var("DB2_TEST_DSN")?;
    let mut db = Db2Session::connect_dsn(&dsn).await?;

    let mut verified: std::collections::HashMap<String, (u64, u64, String)> =
        std::collections::HashMap::new();
    for mode in &modes {
        let (_, _, rows, bytes, checksum) = run_mode(dataset, mode, &mut db, true).await?;
        validate(dataset, rows, &checksum)?;
        verified.insert(mode.clone(), (rows, bytes, checksum));
    }
    let mut samples_by_mode: ModeSamples = std::collections::HashMap::new();
    for mode in &modes {
        for _ in 0..warmups {
            let _ = run_mode(dataset, mode, &mut db, false).await?;
        }
    }
    for iteration in 0..samples {
        for offset in 0..modes.len() {
            let mode = &modes[(iteration + offset) % modes.len()];
            let allocated_before = ALLOCATED.load(Ordering::Relaxed);
            let calls_before = ALLOCS.load(Ordering::Relaxed);
            let (elapsed_ms, first_row_ms, rows, _, _) =
                run_mode(dataset, mode, &mut db, false).await?;
            let allocated_bytes = ALLOCATED.load(Ordering::Relaxed) - allocated_before;
            let allocation_calls = ALLOCS.load(Ordering::Relaxed) - calls_before;
            if rows != dataset.rows {
                return Err(format!(
                    "{}: expected {} rows, got {rows}",
                    dataset.name, dataset.rows
                )
                .into());
            }
            samples_by_mode.entry(mode.clone()).or_default().push((
                elapsed_ms,
                first_row_ms,
                allocated_bytes,
                allocation_calls,
            ));
            let (_, bytes, checksum) = &verified[mode];
            json(&format!(
                "{{\"language\":\"rust\",\"dataset\":\"{}\",\"mode\":\"{}\",\"warmup\":false,\"iteration\":{},\"rows\":{rows},\"bytes\":{bytes},\"checksum\":\"{checksum}\",\"elapsedMs\":{elapsed_ms:.3},\"allocatedBytes\":{allocated_bytes},\"allocationCalls\":{allocation_calls},\"firstRowMs\":{}}}",
                dataset.name,
                mode,
                iteration + 1,
                first_row_ms
                    .map(|v| format!("{v:.3}"))
                    .unwrap_or("null".into())
            ));
        }
    }
    let peak = peak_rss_bytes();
    for mode in &modes {
        let values = &samples_by_mode[mode];
        let mut times: Vec<f64> = values.iter().map(|v| v.0).collect();
        times.sort_by(f64::total_cmp);
        let median = times[times.len() / 2];
        let mut firsts: Vec<f64> = values.iter().filter_map(|v| v.1).collect();
        firsts.sort_by(f64::total_cmp);
        let mut allocations: Vec<usize> = values.iter().map(|v| v.2).collect();
        allocations.sort_unstable();
        let mut calls: Vec<usize> = values.iter().map(|v| v.3).collect();
        calls.sort_unstable();
        let median_allocated = allocations[allocations.len() / 2];
        let median_calls = calls[calls.len() / 2];
        let (_, bytes, checksum) = &verified[mode];
        json(&format!(
            "{{\"language\":\"rust\",\"dataset\":\"{}\",\"mode\":\"{}\",\"summary\":true,\"rows\":{},\"bytes\":{},\"checksum\":\"{}\",\"medianMs\":{median:.3},\"minMs\":{:.3},\"maxMs\":{:.3},\"medianFirstRowMs\":{},\"medianAllocatedBytes\":{median_allocated},\"medianAllocationCalls\":{median_calls},\"peakRssBytes\":{peak}}}",
            dataset.name,
            mode,
            dataset.rows,
            bytes,
            checksum,
            times[0],
            times[times.len() - 1],
            firsts
                .get(firsts.len() / 2)
                .map(|v| format!("{v:.3}"))
                .unwrap_or("null".into())
        ));
    }
    db.close().await?;
    Ok(())
}

async fn run_mode(
    dataset: &Dataset,
    mode: &str,
    db: &mut Db2Session,
    checksum: bool,
) -> Result<(f64, Option<f64>, u64, u64, String), Box<dyn std::error::Error>> {
    match mode {
        "native-materialized" => {
            let started = Instant::now();
            let result = db.query(dataset.select).await?;
            let elapsed = started.elapsed().as_secs_f64() * 1000.0;
            let rows = result.rows.len() as u64;
            let (bytes, digest) = if checksum {
                let mut hash = CanonicalHash::default();
                for row in &result.rows {
                    hash.add(dataset.columns, row);
                }
                (hash.bytes, hash.finish())
            } else {
                (0, String::new())
            };
            Ok((elapsed, None, rows, bytes, digest))
        }
        "native-stream" => {
            let started = Instant::now();
            let mut reader = db.query_stream(dataset.select).await?;
            let mut hash = if checksum {
                Some(CanonicalHash::default())
            } else {
                None
            };
            let mut first_row = None;
            let mut rows = 0u64;
            while let Some(mut row) = reader.next().await? {
                if first_row.is_none() {
                    first_row = Some(started.elapsed().as_secs_f64() * 1000.0);
                }
                materialize_lobs(&mut reader, &mut row).await?;
                if let Some(hash) = hash.as_mut() {
                    hash.add(dataset.columns, &row);
                }
                rows += 1;
            }
            let elapsed = started.elapsed().as_secs_f64() * 1000.0;
            let (bytes, digest) = match hash {
                Some(hash) => (hash.bytes, hash.finish()),
                None => (0, String::new()),
            };
            Ok((elapsed, first_row, rows, bytes, digest))
        }
        other => Err(format!("unknown mode {other}").into()),
    }
}

async fn materialize_lobs(
    reader: &mut Db2RowReader<'_>,
    row: &mut [Db2Value],
) -> Result<(), Box<dyn std::error::Error>> {
    for slot in row.iter_mut() {
        let value = match slot {
            Db2Value::Lob(value) => *value,
            _ => continue,
        };
        let mut bytes = Vec::new();
        let mut lob_reader = reader.open_lob(&value).await?;
        while let Some(chunk) = lob_reader.next_chunk().await? {
            bytes.extend_from_slice(&chunk);
        }
        *slot = if value.encoding == Db2LobEncoding::Binary {
            Db2Value::Bytes(bytes)
        } else {
            Db2Value::Text(String::from_utf8_lossy(&bytes).into_owned())
        };
    }
    Ok(())
}

fn validate(
    dataset: &Dataset,
    rows: u64,
    checksum: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if rows != dataset.rows {
        return Err(format!(
            "{}: expected {} rows, got {rows}",
            dataset.name, dataset.rows
        )
        .into());
    }
    if checksum != dataset.expected {
        return Err(format!(
            "{}: checksum {checksum} does not match manifest {}",
            dataset.name, dataset.expected
        )
        .into());
    }
    Ok(())
}
