//! Go2Parity: Rust port of go-db2/benchmarks/suite/{conformance,perf}.go.
//! Baseline in the report is go-db2; this example emits the rust column.
//! Contract matches cs-db2/benchmarks/Go2Parity/Program.cs and
//! ts-db2/benchmarks/go2-parity.cjs; aggregated by run-db2-go2-benchmark.cjs.
//!
//! Usage: DB2_TEST_DSN=... cargo run --release --example go2_parity -- \
//!   [--warmups 3] [--samples 7] [--fast] [--seed-only] [--no-seed]
use justybase_db2::{Db2Session, Db2Value};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

struct CountingAllocator;
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATED.fetch_add(new_size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const CREATE_CONF: &str = "CREATE TABLE BENCH_CONF_TYPES (ID INT NOT NULL PRIMARY KEY, C_SMALLINT SMALLINT, C_INT INTEGER, C_BIGINT BIGINT, C_REAL REAL, C_DOUBLE DOUBLE, C_DECIMAL DECIMAL(15,2), C_VARCHAR VARCHAR(100), C_DECFLOAT DECFLOAT(34), C_BLOB BLOB(64K), C_CLOB CLOB(64K), C_VARGRAPHIC VARGRAPHIC(100))";
const INSERT_CONF: &str = "INSERT INTO BENCH_CONF_TYPES (ID, C_SMALLINT, C_INT, C_BIGINT, C_REAL, C_DOUBLE, C_DECIMAL, C_VARCHAR, C_DECFLOAT, C_BLOB, C_CLOB, C_VARGRAPHIC) VALUES (1, 32767, 2147483647, 9223372036854775807, 3.1415, 2.718281828459, 1234567.89, 'Universal Db2 Conformance Suite', DECFLOAT('12345678901234567890.12345678901234', 34), ?, ?, 'Unicode / \u{65e5}\u{672c}\u{8a9e} / Portugu\u{00ea}s')";

fn blob_seed() -> Vec<u8> {
    let mut buf = Vec::with_capacity(1024);
    for _ in 0..256 {
        buf.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    }
    buf
}

fn perf_blob() -> Vec<u8> {
    let unit = b"PureGoDb2DriverBenchmarkPayload1234567890!";
    let mut buf = Vec::with_capacity(unit.len() * 12000);
    for _ in 0..12000 {
        buf.extend_from_slice(unit);
    }
    buf
}

fn clob_seed() -> String {
    "Pure-Go-Db2-CLOB-Test-Payload-".repeat(40)
}

fn as_i64(v: &Db2Value) -> i64 {
    match v {
        Db2Value::Integer(x) => *x,
        Db2Value::Float(x) => *x as i64,
        Db2Value::Decimal(s) | Db2Value::Text(s) => s.trim().parse().unwrap_or(0),
        Db2Value::Boolean(b) => i64::from(*b),
        _ => 0,
    }
}

fn as_f64(v: &Db2Value) -> f64 {
    match v {
        Db2Value::Float(x) => *x,
        Db2Value::Integer(x) => *x as f64,
        Db2Value::Decimal(s) | Db2Value::Text(s) => s.trim().parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn as_text(v: &Db2Value) -> String {
    match v {
        Db2Value::Text(s) | Db2Value::Decimal(s) => s.trim().to_string(),
        Db2Value::Integer(x) => x.to_string(),
        Db2Value::Float(x) => x.to_string(),
        Db2Value::Boolean(b) => b.to_string(),
        Db2Value::Bytes(b) => String::from_utf8_lossy(b).trim().to_string(),
        Db2Value::Null => String::new(),
        Db2Value::Lob(_) | Db2Value::ProgressiveLobReference { .. } => String::from("<lob>"),
    }
}

fn emit(payload: JsonLine) {
    println!("{payload}");
}

// Minimal JSON emitter without serde dependency (examples must stay lean).
struct JsonLine {
    body: String,
}
impl std::fmt::Display for JsonLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.body)
    }
}
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

async fn seed(db: &mut Db2Session) -> Result<(), Box<dyn std::error::Error>> {
    for ddl in [
        "DROP TABLE BENCH_CONF_TYPES",
        "DROP TABLE BENCH_PERF_ROWS",
        "DROP TABLE BENCH_PERF_ROWS_100K",
        "DROP TABLE BENCH_PERF_INSERT",
        "DROP TABLE BENCH_PERF_LOB",
    ] {
        let _ = db.execute(ddl).await;
    }
    db.execute(CREATE_CONF).await?;
    db.execute(
        "CREATE TABLE BENCH_PERF_ROWS (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(50), VAL DOUBLE)",
    )
    .await?;
    db.execute("CREATE TABLE BENCH_PERF_ROWS_100K (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(50), VAL DOUBLE)")
        .await?;
    db.execute("CREATE TABLE BENCH_PERF_INSERT (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(50), VAL DOUBLE)")
        .await?;
    db.execute("CREATE TABLE BENCH_PERF_LOB (ID INT NOT NULL PRIMARY KEY, DATA BLOB(1M))")
        .await?;
    db.execute_with_params(
        INSERT_CONF,
        &[Db2Value::Bytes(blob_seed()), Db2Value::Text(clob_seed())],
    )
    .await?;
    let stmt = db
        .prepare("INSERT INTO BENCH_PERF_ROWS (ID, NAME, VAL) VALUES (?, ?, ?)")
        .await?;
    for i in 1..=1000i64 {
        db.execute_prepared(
            &stmt,
            &[
                Db2Value::Integer(i),
                Db2Value::Text(format!("Customer-Record-{i:05}")),
                Db2Value::Float(i as f64 * 1.5),
            ],
        )
        .await?;
    }
    // 100k rows via recursive CTE (server-side, ~0.6s); cross-join ROW_NUMBER takes ~2min.
    db.execute(
        "INSERT INTO BENCH_PERF_ROWS_100K (ID, NAME, VAL) \
         WITH n(i) AS (VALUES (1) UNION ALL SELECT i+1 FROM n WHERE i < 100000) \
         SELECT i, 'Customer-Record-' || CHAR(i), DOUBLE(i)*1.5 FROM n",
    )
    .await?;
    db.execute_with_params(
        "INSERT INTO BENCH_PERF_LOB (ID, DATA) VALUES (1, ?)",
        &[Db2Value::Bytes(perf_blob())],
    )
    .await?;
    Ok(())
}

async fn conformance_row(
    db: &mut Db2Session,
    name: &str,
    sql: &str,
    fmt: impl Fn(&[Db2Value]) -> String,
) -> (String, String, bool, Option<String>) {
    match db.query(sql).await {
        Ok(r) => {
            let out = fmt(r.rows.first().map(Vec::as_slice).unwrap_or(&[]));
            (name.to_string(), out, true, None)
        }
        Err(e) => (
            name.to_string(),
            format!("ERR: {e:?}").chars().take(120).collect(),
            false,
            Some(format!("{e}").chars().take(300).collect()),
        ),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut warmups = 3usize;
    let mut samples = 7usize;
    let mut fast = false;
    let mut seed_only = false;
    let mut no_seed = false;
    let mut it = std::env::args().skip(1).peekable();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--warmups" => warmups = it.next().unwrap_or_default().parse().unwrap_or(3),
            "--samples" => samples = it.next().unwrap_or_default().parse().unwrap_or(7),
            "--fast" => fast = true,
            "--seed-only" => seed_only = true,
            "--no-seed" => no_seed = true,
            _ => {}
        }
    }
    if fast {
        warmups = warmups.min(1);
        samples = samples.min(3);
    }
    let iters = if fast {
        [5, 5, 2, 2, 3, 2]
    } else {
        [500, 1000, 50, 10, 300, 50]
    };

    let dsn = std::env::var("DB2_TEST_DSN")?;
    let mut db = Db2Session::connect_dsn(&dsn).await?;
    if !no_seed {
        seed(&mut db).await?;
    }
    if seed_only {
        return Ok(());
    }
    emit(JsonLine {
        body: r#"{"language":"rust","suite":"hello","dsn":"REDACTED"}"#.to_string(),
    });

    let mut rows: Vec<(String, String, bool, Option<String>)> = Vec::new();
    rows.push(
        conformance_row(
            &mut db,
            "Basic Scalar (Int, String, Float)",
            "SELECT 42, 'Pure Go Db2 Driver', 123.45 FROM SYSIBM.SYSDUMMY1",
            |r| {
                format!(
                    "i={}, s=\"{}\", f={:.2}",
                    as_i64(&r[0]),
                    as_text(&r[1]),
                    as_f64(&r[2])
                )
            },
        )
        .await,
    );
    rows.push(
        conformance_row(
            &mut db,
            "Null Handling (sql.Null*)",
            "SELECT CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(50)) FROM SYSIBM.SYSDUMMY1",
            |r| {
                format!(
                    "ni.Valid={}, ns.Valid={}",
                    !matches!(r[0], Db2Value::Null),
                    !matches!(r[1], Db2Value::Null)
                )
            },
        )
        .await,
    );
    rows.push(
        conformance_row(
            &mut db,
            "Temporal (Current Date Format)",
            "SELECT VARCHAR_FORMAT(CURRENT DATE, 'YYYY-MM-DD') FROM SYSIBM.SYSDUMMY1",
            |r| as_text(&r[0]),
        )
        .await,
    );
    rows.push(
        conformance_row(
            &mut db,
            "Numeric Precision (Smallint, Int, Bigint)",
            "SELECT C_SMALLINT, C_INT, C_BIGINT FROM BENCH_CONF_TYPES WHERE ID = 1",
            |r| {
                format!(
                    "s={}, i={}, b={}",
                    as_i64(&r[0]),
                    as_i64(&r[1]),
                    as_i64(&r[2])
                )
            },
        )
        .await,
    );
    rows.push(
        conformance_row(
            &mut db,
            "High Precision Decimal & DECFLOAT",
            "SELECT VARCHAR(C_DECIMAL), VARCHAR(C_DECFLOAT) FROM BENCH_CONF_TYPES WHERE ID = 1",
            |r| format!("dec={}, decfloat={}", as_text(&r[0]), as_text(&r[1])),
        )
        .await,
    );
    // BLOB SHA-256 with expected-hash check like conformance.go.
    rows.push(
        async {
            let name = "BLOB Binary Integrity (SHA-256)";
            match db
                .query("SELECT C_BLOB FROM BENCH_CONF_TYPES WHERE ID = 1")
                .await
            {
                Ok(r) => {
                    let bytes = match &r.rows[0][0] {
                        Db2Value::Bytes(b) => b.clone(),
                        _ => vec![],
                    };
                    let actual = sha256_hex(&bytes);
                    let expected = sha256_hex(&blob_seed());
                    if actual == expected {
                        (name.to_string(), actual, true, None)
                    } else {
                        let d = format!("Expected hash {expected}, got {actual}");
                        (name.to_string(), actual, false, Some(d))
                    }
                }
                Err(e) => (
                    name.to_string(),
                    format!("ERR: {e:?}"),
                    false,
                    Some(format!("{e}")),
                ),
            }
        }
        .await,
    );
    rows.push(
        conformance_row(
            &mut db,
            "CLOB Text Streaming",
            "SELECT LENGTH(C_CLOB), SUBSTR(C_CLOB, 1, 30) FROM BENCH_CONF_TYPES WHERE ID = 1",
            |r| format!("len={}, prefix={}", as_i64(&r[0]), as_text(&r[1])),
        )
        .await,
    );
    rows.push(
        conformance_row(
            &mut db,
            "VARGRAPHIC International Strings",
            "SELECT C_VARGRAPHIC FROM BENCH_CONF_TYPES WHERE ID = 1",
            |r| as_text(&r[0]),
        )
        .await,
    );
    for (name, output, passed, diff) in &rows {
        emit(JsonLine {
            body: format!(
                "{{\"language\":\"rust\",\"suite\":\"conformance\",\"scenario\":\"{}\",\"output\":\"{}\",\"passed\":{},\"diff\":{}}}",
                json_escape(name),
                json_escape(output),
                passed,
                diff.as_ref()
                    .map(|d| format!("\"{}\"", json_escape(d)))
                    .unwrap_or("null".to_string())
            ),
        });
    }

    let scenarios: [(&str, usize); 6] = [
        ("Ping (Round-trip Latency)", iters[0]),
        ("Simple Single-Row SELECT", iters[1]),
        ("Fetch 1,000 Rows Scan", iters[2]),
        ("Fetch 100,000 Rows Scan", iters[3]),
        ("Prepared Statement INSERT", iters[4]),
        ("Read 500KB BLOB Payload", iters[5]),
    ];
    for (idx, (scenario, n)) in scenarios.iter().enumerate() {
        for _ in 0..warmups {
            run_perf_once(&mut db, idx, *n).await?;
        }
        let mut vals: Vec<(f64, usize)> = Vec::new();
        for i in 0..samples {
            ALLOCATED.store(0, Ordering::Relaxed);
            let t0 = Instant::now();
            for _ in 0..*n {
                run_perf_once(&mut db, idx, 1).await?;
            }
            // run_perf_once with inner=1 performs a single op; divide by n for per-op.
            let ms = t0.elapsed().as_secs_f64() * 1000.0 / (*n as f64);
            let alloc = ALLOCATED.load(Ordering::Relaxed) / (*n).max(1);
            vals.push((ms, alloc));
            emit(JsonLine {
                body: format!(
                    "{{\"language\":\"rust\",\"suite\":\"perf\",\"scenario\":\"{}\",\"mode\":\"native-materialized\",\"warmup\":false,\"iteration\":{},\"ops\":{},\"elapsedMs\":{},\"allocatedBytes\":{}}}",
                    json_escape(scenario),
                    i + 1,
                    n,
                    ms,
                    alloc
                ),
            });
        }
        vals.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut allocs: Vec<usize> = vals.iter().map(|v| v.1).collect();
        allocs.sort_unstable();
        let peak = peak_rss();
        emit(JsonLine {
            body: format!(
                "{{\"language\":\"rust\",\"suite\":\"perf\",\"scenario\":\"{}\",\"mode\":\"native-materialized\",\"summary\":true,\"ops\":{},\"medianMs\":{},\"minMs\":{},\"maxMs\":{},\"medianAllocatedBytes\":{},\"peakRssBytes\":{}}}",
                json_escape(scenario),
                n,
                vals[vals.len() / 2].0,
                vals[0].0,
                vals[vals.len() - 1].0,
                allocs[allocs.len() / 2],
                peak
            ),
        });
    }

    if rows.iter().any(|r| !r.2) {
        std::process::exit(1);
    }
    Ok(())
}

async fn run_perf_once(
    db: &mut Db2Session,
    scenario_idx: usize,
    inner: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // inner is 1 from the timed loop (per-op already divided); the warmup path
    // passes the full n so warmup cost matches other languages.
    let reps = inner.max(1);
    for _ in 0..reps {
        match scenario_idx {
            0 => {
                db.ping().await?;
            }
            1 => {
                let r = db.query("SELECT 1 FROM SYSIBM.SYSDUMMY1").await?;
                if as_i64(&r.rows[0][0]) != 1 {
                    return Err("expected 1".into());
                }
            }
            2 => {
                let r = db
                    .query("SELECT ID, NAME, VAL FROM BENCH_PERF_ROWS")
                    .await?;
                if r.rows.len() != 1000 {
                    return Err(format!("expected 1000 rows, got {}", r.rows.len()).into());
                }
                for row in &r.rows {
                    let _ = as_i64(&row[0]);
                    let _ = as_text(&row[1]);
                    let _ = as_f64(&row[2]);
                }
            }
            3 => {
                let r = db
                    .query("SELECT ID, NAME, VAL FROM BENCH_PERF_ROWS_100K")
                    .await?;
                if r.rows.len() != 100000 {
                    return Err(format!("expected 100000 rows, got {}", r.rows.len()).into());
                }
                for row in &r.rows {
                    let _ = as_i64(&row[0]);
                    let _ = as_text(&row[1]);
                    let _ = as_f64(&row[2]);
                }
            }
            4 => {
                let _ = db.execute("DELETE FROM BENCH_PERF_INSERT").await;
                let stmt = db
                    .prepare("INSERT INTO BENCH_PERF_INSERT (ID, NAME, VAL) VALUES (?, ?, ?)")
                    .await?;
                for i in 1..=10i64 {
                    db.execute_prepared(
                        &stmt,
                        &[
                            Db2Value::Integer(i),
                            Db2Value::Text("test_name".to_string()),
                            Db2Value::Float(i as f64 * 2.5),
                        ],
                    )
                    .await?;
                }
            }
            5 => {
                let r = db
                    .query("SELECT DATA FROM BENCH_PERF_LOB WHERE ID = 1")
                    .await?;
                match &r.rows[0][0] {
                    Db2Value::Bytes(_) => {}
                    _ => return Err("expected BLOB bytes".into()),
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn sha256_hex(data: &[u8]) -> String {
    // Small self-contained SHA-256 (no extra deps for the example).
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[4 * i],
                chunk[4 * i + 1],
                chunk[4 * i + 2],
                chunk[4 * i + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = String::with_capacity(64);
    for v in h {
        out.push_str(&format!("{v:08x}"));
    }
    out
}

fn peak_rss() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|t| {
            t.lines().find(|l| l.starts_with("VmHWM:")).and_then(|l| {
                l.split_whitespace()
                    .nth(1)?
                    .parse::<u64>()
                    .ok()
                    .map(|kb| kb * 1024)
            })
        })
        .unwrap_or(0)
}
