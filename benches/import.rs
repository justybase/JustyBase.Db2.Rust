//! Import-pipeline encode benchmarks: 128-row batches mirroring the C#
//! importer's `BatchSize = 128` default, plus the 8 MiB soft byte boundary.
//! Timing uses Criterion; allocation figures use `stats_alloc` regions printed
//! to stderr (visible in bench logs) and recorded in BENCHMARK-BASELINE.md.
//! Only the public crate surface is used, so no `bench-internals` is required.
use std::{alloc::System, time::Instant};

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use justybase_db2::{
    Db2ImportColumn, Db2ImportTarget, Db2Value, converters::sql_dta,
    network::ddm::ColumnDescription,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn column(sql_type: u16, length: u64, precision: u16, scale: u16) -> ColumnDescription {
    ColumnDescription {
        name: String::new(),
        sql_type,
        length,
        precision,
        scale,
        nullable: true,
    }
}

/// Encodes `rows` batches of `columns`/`make_row` and reports allocations per
/// batch alongside Criterion timing.
fn bench_batch(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    name: &str,
    columns: Vec<ColumnDescription>,
    make_row: impl Fn() -> Vec<Db2Value> + Copy,
) {
    const ROWS_PER_BATCH: usize = 128;
    group.throughput(Throughput::Elements(ROWS_PER_BATCH as u64));
    group.bench_function(BenchmarkId::new("batch_128", name), |b| {
        b.iter_custom(|iters| {
            let region = Region::new(GLOBAL);
            let start = Instant::now();
            let mut bytes = 0usize;
            for _ in 0..iters {
                for _ in 0..ROWS_PER_BATCH {
                    let row = make_row();
                    let encoded =
                        sql_dta::build_sql_dta(black_box(&columns), black_box(&row)).unwrap();
                    bytes += encoded.len();
                }
            }
            let elapsed = start.elapsed();
            let stats = region.change();
            let batches = iters as f64;
            eprintln!(
                "alloc batch_128/{name}: batches={iters} bytes_per_batch={} allocations_per_batch={:.1} bytes_allocated_per_batch={}",
                bytes / iters as usize,
                stats.allocations as f64 / batches,
                stats.bytes_allocated / iters as usize,
            );
            black_box(bytes);
            elapsed
        });
    });
}

fn bench_import_batch_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("import_batch_encode");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_millis(100));
    group.measurement_time(std::time::Duration::from_millis(250));

    let integer_columns = vec![column(497, 4, 0, 0); 10];
    bench_batch(&mut group, "10_integer", integer_columns, || {
        (0..10).map(Db2Value::Integer).collect()
    });

    let mixed_columns = vec![
        column(497, 4, 0, 0),
        column(449, 20, 0, 0),
        column(485, 16, 20, 5),
        column(909, 32, 0, 0),
    ];
    bench_batch(&mut group, "mixed_4", mixed_columns, || {
        vec![
            Db2Value::Integer(42),
            Db2Value::Text("value-0123456789ab".into()),
            Db2Value::Decimal("1234567890.12345".into()),
            Db2Value::Bytes(vec![0x5a; 32]),
        ]
    });

    // One 60 KiB VARCHAR per row reaches ~7.7 MiB per 128-row batch, just under
    // the 8 MiB soft import boundary a single oversized row may exceed alone.
    let wide_columns = vec![column(449, 60_000, 0, 0)];
    bench_batch(&mut group, "varchar_60k", wide_columns, || {
        vec![Db2Value::Text("w".repeat(60_000))]
    });

    group.finish();
}

fn bench_import_target_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("import_target_build");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_millis(100));
    group.measurement_time(std::time::Duration::from_millis(250));
    group.bench_function("target_50_columns", |b| {
        b.iter(|| {
            let target = Db2ImportTarget::new(
                Some("IMPORT_TEST"),
                "DESTINATION",
                (0..50)
                    .map(|index| Db2ImportColumn::new(format!("COL_{index:02}")))
                    .collect(),
            )
            .unwrap();
            black_box(target.insert_sql().len())
        });
    });
    group.finish();
}

criterion_group!(
    import_pipeline,
    bench_import_batch_encode,
    bench_import_target_build
);
criterion_main!(import_pipeline);
