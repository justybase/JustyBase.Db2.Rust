use std::{alloc::System, env, hint::black_box};

use justybase_db2::{
    Db2Value,
    converters::sql_dta,
    network::{
        ddm::{ColumnDescription, FieldDescriptor},
        session,
    },
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

fn measure(label: &str, iterations: usize, mut operation: impl FnMut()) {
    let region = Region::new(GLOBAL);
    for _ in 0..iterations {
        operation();
    }
    let stats = region.change();
    println!(
        "case={label} iterations={iterations} allocations_per_op={:.1} reallocations_per_op={:.1} bytes_allocated_per_op={} bytes_reallocated_net_per_op={:.1}",
        stats.allocations as f64 / iterations as f64,
        stats.reallocations as f64 / iterations as f64,
        stats.bytes_allocated / iterations,
        stats.bytes_reallocated as f64 / iterations as f64,
    );
}

fn qrydta(fields: &[FieldDescriptor], rows: usize) -> Vec<u8> {
    let row_size = 2 + fields
        .iter()
        .map(|field| match field.field_type {
            0x02 => 4,
            0x40 => 2 + 20,
            0xc2 => 2 + 32,
            0x0e => 5,
            0xba => 8,
            _ => panic!("unsupported allocation-probe field type"),
        })
        .sum::<usize>();
    let mut data = Vec::with_capacity(row_size * rows);
    for row in 0..rows {
        data.extend_from_slice(&[0xff, 0]);
        for field in fields {
            match field.field_type {
                0x02 => data.extend_from_slice(&(row as i32).to_le_bytes()),
                0x40 => {
                    data.extend_from_slice(&20u16.to_be_bytes());
                    data.extend_from_slice(b"abcdefghijklmnopqrst");
                }
                0xc2 => {
                    data.extend_from_slice(&32u16.to_be_bytes());
                    data.extend_from_slice(&[0x5a; 32]);
                }
                0x0e => data.extend_from_slice(&[0x12, 0x34, 0x56, 0x78, 0x9c]),
                0xba => data.extend_from_slice(&[0x22, 0x2c, 0, 0, 0, 2, 0x8e, 0x56]),
                _ => unreachable!(),
            }
        }
    }
    data
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let iterations = env::args()
        .nth(1)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(5);
    if iterations == 0 {
        return Err("iterations must be positive".into());
    }

    let integer_columns = vec![column(497, 4, 0, 0); 10];
    let integer_values = (0..10).map(Db2Value::Integer).collect::<Vec<_>>();
    measure("encode_10_integer", iterations, || {
        black_box(
            sql_dta::build_sql_dta(&integer_columns, &integer_values)
                .expect("integer SQLDTA should encode"),
        );
    });

    let short_text_columns = vec![column(449, 20, 0, 0); 10];
    let short_text_values = (0..10)
        .map(|index| Db2Value::Text(format!("value-{index:013}")))
        .collect::<Vec<_>>();
    measure("encode_10_varchar_20", iterations, || {
        black_box(
            sql_dta::build_sql_dta(&short_text_columns, &short_text_values)
                .expect("short-text SQLDTA should encode"),
        );
    });

    for (label, size) in [("varchar_1k", 1024usize), ("varchar_64k", 32_768)] {
        let columns = [column(449, size as u64, 0, 0)];
        let values = [Db2Value::Text("v".repeat(size))];
        measure(label, iterations, || {
            black_box(
                sql_dta::build_sql_dta(&columns, &values).expect("VARCHAR SQLDTA should encode"),
            );
        });
    }

    for size in [1024usize, 65_535] {
        let columns = [column(909, size as u64, 0, 0)];
        let values = [Db2Value::Bytes(vec![0x5a; size])];
        measure(&format!("encode_varbinary_{size}"), iterations, || {
            black_box(
                sql_dta::build_sql_dta(&columns, &values).expect("VARBINARY SQLDTA should encode"),
            );
        });
    }

    let decimal_columns = [column(485, 16, 20, 5)];
    let decimal_values = [Db2Value::Decimal("1234567890.12345".into())];
    measure("encode_decimal", iterations, || {
        black_box(
            sql_dta::build_sql_dta(&decimal_columns, &decimal_values)
                .expect("DECIMAL SQLDTA should encode"),
        );
    });

    let decfloat_columns = [column(997, 16, 0, 0)];
    let decfloat_values = [Db2Value::Decimal(
        "12345678901234567890.12345678901234".into(),
    )];
    measure("encode_decfloat34", iterations, || {
        black_box(
            sql_dta::build_sql_dta(&decfloat_columns, &decfloat_values)
                .expect("DECFLOAT SQLDTA should encode"),
        );
    });

    let blob_columns = [column(405, 1_048_576, 0, 0)];
    let blob_values = [Db2Value::Bytes(vec![0xa5; 1_048_576])];
    measure("encode_blob_1m", iterations, || {
        black_box(
            sql_dta::build_sql_dta(&blob_columns, &blob_values).expect("BLOB SQLDTA should encode"),
        );
    });

    for count in [10usize, 100, 1000] {
        let columns = vec![column(497, 4, 0, 0); count];
        let values = (0..count)
            .map(|index| Db2Value::Integer(index as i64))
            .collect::<Vec<_>>();
        measure(
            &format!("encode_sqldta_{count}_parameters"),
            iterations,
            || {
                black_box(
                    sql_dta::build_sql_dta(&columns, &values)
                        .expect("multi-parameter SQLDTA should encode"),
                );
            },
        );
    }

    let integer = FieldDescriptor {
        field_type: 0x02,
        precision_scale: [0, 4],
    };
    let varchar = FieldDescriptor {
        field_type: 0x40,
        precision_scale: [0, 0],
    };
    let binary = FieldDescriptor {
        field_type: 0xc2,
        precision_scale: [0, 0],
    };
    let decimal = FieldDescriptor {
        field_type: 0x0e,
        precision_scale: [9, 2],
    };
    let decfloat = FieldDescriptor {
        field_type: 0xba,
        precision_scale: [0, 8],
    };
    let scenarios = [
        ("integer_1c_100k", vec![integer.clone()]),
        ("integer_10c_100k", vec![integer.clone(); 10]),
        (
            "integer_varchar_100k",
            vec![integer.clone(), varchar.clone()],
        ),
        ("varchar_10c_100k", vec![varchar.clone(); 10]),
        ("varbinary_100k", vec![binary.clone()]),
        ("decimal_100k", vec![decimal.clone()]),
        ("decfloat_100k", vec![decfloat.clone()]),
        (
            "mixed_100k",
            vec![integer, varchar, binary, decimal, decfloat],
        ),
    ];
    for (label, fields) in scenarios {
        let data = qrydta(&fields, 100_000);
        measure(label, 1, || {
            black_box(
                session::benchmark_decode_rows(&fields, &data)
                    .expect("synthetic QRYDTA should decode"),
            );
        });
        measure(&format!("{label}_flat"), 1, || {
            black_box(
                session::benchmark_decode_rows_flat(&fields, &data)
                    .expect("synthetic QRYDTA should decode to flat rows"),
            );
        });
    }

    Ok(())
}
