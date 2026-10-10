use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use justybase_db2::{
    Db2Value,
    converters::{decfloat, sql_dta},
    network::{
        codepoint as cp,
        ddm::{self, ColumnDescription, FieldDescriptor},
        dss, session,
    },
};
use std::{
    collections::VecDeque,
    io::{Cursor, IoSlice},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;

#[derive(Default)]
struct CountingWriter {
    bytes_written: usize,
    write_calls: usize,
}

impl tokio::io::AsyncWrite for CountingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        this.bytes_written += buffer.len();
        this.write_calls += 1;
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let written = buffers.iter().map(|buffer| buffer.len()).sum();
        this.bytes_written += written;
        this.write_calls += 1;
        Poll::Ready(Ok(written))
    }
}

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

fn bench_value_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_dta_encode");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(250));

    let integer_columns = vec![column(497, 4, 0, 0); 10];
    let integer_values = (0..10).map(Db2Value::Integer).collect::<Vec<_>>();
    group.bench_function("10_integer_parameters", |b| {
        b.iter(|| {
            black_box(
                sql_dta::build_sql_dta(black_box(&integer_columns), black_box(&integer_values))
                    .unwrap(),
            )
        });
    });

    let varchar_columns = vec![column(449, 20, 0, 0); 10];
    let varchar_values = (0..10)
        .map(|index| Db2Value::Text(format!("value-{index:02}abcde")))
        .collect::<Vec<_>>();
    group.throughput(Throughput::Elements(10));
    group.bench_function("10_varchar_20", |b| {
        b.iter(|| {
            black_box(
                sql_dta::build_sql_dta(black_box(&varchar_columns), black_box(&varchar_values))
                    .unwrap(),
            )
        });
    });

    for (name, size) in [
        ("varchar_20", 20usize),
        ("varchar_1k", 1024),
        ("varchar_64k", 32_768),
    ] {
        let value = Db2Value::Text("v".repeat(size));
        let encoded_size = size * 2 + 8;
        let columns = [column(449, size as u64, 0, 0)];
        group.throughput(Throughput::Bytes(encoded_size as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                black_box(
                    sql_dta::build_sql_dta(
                        black_box(&columns),
                        black_box(std::slice::from_ref(&value)),
                    )
                    .unwrap(),
                )
            });
        });
    }

    // The DRDA variable-binary length field is u16, so 65,535 bytes is the
    // largest representable value adjacent to the requested 64 KiB case.
    for size in [1024usize, 65_535] {
        let value = Db2Value::Bytes(vec![0x5a; size]);
        let columns = [column(909, size as u64, 0, 0)];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(BenchmarkId::new("varbinary", size), |b| {
            b.iter(|| {
                black_box(
                    sql_dta::build_sql_dta(
                        black_box(&columns),
                        black_box(std::slice::from_ref(&value)),
                    )
                    .unwrap(),
                )
            });
        });
    }

    let decimal_column = [column(485, 16, 20, 5)];
    let decimal_value = [Db2Value::Decimal("1234567890.12345".into())];
    group.bench_function("decimal", |b| {
        b.iter(|| {
            black_box(
                sql_dta::build_sql_dta(black_box(&decimal_column), black_box(&decimal_value))
                    .unwrap(),
            )
        });
    });

    let decfloat_column = [column(997, 16, 0, 0)];
    let decfloat_value = [Db2Value::Decimal(
        "12345678901234567890.12345678901234".into(),
    )];
    group.bench_function("decfloat34", |b| {
        b.iter(|| {
            black_box(
                sql_dta::build_sql_dta(black_box(&decfloat_column), black_box(&decfloat_value))
                    .unwrap(),
            )
        });
    });

    let blob_column = [column(405, 1_048_576, 0, 0)];
    let blob_value = [Db2Value::Bytes(vec![0xa5; 1_048_576])];
    group.throughput(Throughput::Bytes(1_048_576));
    group.bench_function("blob_1m", |b| {
        b.iter(|| {
            black_box(
                sql_dta::build_sql_dta_with_external_data(
                    black_box(&blob_column),
                    black_box(&blob_value),
                )
                .unwrap(),
            )
        });
    });

    for parameter_count in [10usize, 100, 1000] {
        let columns = vec![column(497, 4, 0, 0); parameter_count];
        let values = (0..parameter_count)
            .map(|value| Db2Value::Integer(value as i64))
            .collect::<Vec<_>>();
        group.throughput(Throughput::Elements(parameter_count as u64));
        group.bench_function(
            BenchmarkId::new("sql_dta_parameters", parameter_count),
            |b| {
                b.iter(|| {
                    black_box(
                        sql_dta::build_sql_dta(black_box(&columns), black_box(&values)).unwrap(),
                    )
                });
            },
        );
    }
    group.finish();

    let mut decimal_group = c.benchmark_group("decfloat_encode_decode");
    for (size, input) in [
        (8usize, "123456789012345.6"),
        (16usize, "12345678901234567890123456789012.34"),
    ] {
        let encoded = decfloat::encode_dfp(input, size).unwrap();
        decimal_group.bench_function(BenchmarkId::new("encode", size), |b| {
            b.iter(|| black_box(decfloat::encode_dfp(black_box(input), size).unwrap()));
        });
        decimal_group.bench_function(BenchmarkId::new("decode", size), |b| {
            b.iter(|| black_box(decfloat::decode_dfp(black_box(&encoded)).unwrap()));
        });
    }
    decimal_group.finish();
}

fn qrydta(fields: &[FieldDescriptor], rows: usize) -> Vec<u8> {
    let mut data = Vec::new();
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
                _ => panic!("unsupported synthetic benchmark field type"),
            }
        }
    }
    data
}

fn bench_row_decoding(c: &mut Criterion) {
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
        ("integer_1c", vec![integer.clone()]),
        ("integer_10c", vec![integer.clone(); 10]),
        ("integer_varchar", vec![integer.clone(), varchar.clone()]),
        ("varchar_10c", vec![varchar.clone(); 10]),
        ("varbinary", vec![binary.clone()]),
        ("decimal", vec![decimal.clone()]),
        ("decfloat", vec![decfloat.clone()]),
        (
            "mixed",
            vec![integer.clone(), varchar.clone(), binary, decimal, decfloat],
        ),
    ];
    let mut group = c.benchmark_group("qrydta_decode");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(250));
    for (name, fields) in scenarios {
        let data = qrydta(&fields, 100_000);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                black_box(
                    session::benchmark_decode_rows(black_box(&fields), black_box(&data)).unwrap(),
                )
            });
        });
        group.bench_function(BenchmarkId::new("flat", name), |b| {
            b.iter(|| {
                black_box(
                    session::benchmark_decode_rows_flat(black_box(&fields), black_box(&data))
                        .unwrap(),
                )
            });
        });
    }

    let fields = [integer];
    let complete = qrydta(&fields, 100_000);
    let mut split_data = complete.clone();
    split_data.extend_from_slice(&[0xff, 0, 0x34, 0x12]);
    let fragments = [
        &split_data[..split_data.len() - 2],
        &split_data[split_data.len() - 2..],
    ];
    group.throughput(Throughput::Bytes(split_data.len() as u64));
    group.bench_function("full_rows_plus_half_next_row", |b| {
        b.iter(|| {
            black_box(
                session::benchmark_decode_row_fragments(black_box(&fields), black_box(&fragments))
                    .unwrap(),
            )
        });
    });

    let mut random_fragments = Vec::new();
    let mut seed = 0x9e37_79b9_u32;
    let mut offset = 0;
    while offset < complete.len() {
        seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let amount = ((seed as usize % 4096) + 1).min(complete.len() - offset);
        random_fragments.push(&complete[offset..offset + amount]);
        offset += amount;
    }
    group.throughput(Throughput::Bytes(complete.len() as u64));
    group.bench_function("random_packet_boundaries", |b| {
        b.iter(|| {
            black_box(
                session::benchmark_decode_row_fragments(
                    black_box(&fields),
                    black_box(&random_fragments),
                )
                .unwrap(),
            )
        });
    });
    group.finish();

    let queue: VecDeque<Vec<u8>> = (0..4096).map(|_| Vec::new()).collect();
    let mut queue_group = c.benchmark_group("extdta_queue_advance");
    queue_group.sample_size(20);
    queue_group.warm_up_time(Duration::from_millis(100));
    queue_group.measurement_time(Duration::from_millis(500));
    for consumed in [1usize, 256, 2048] {
        queue_group.bench_function(BenchmarkId::new("drain", consumed), |b| {
            b.iter_batched(
                || queue.clone(),
                |mut extdta| {
                    drop(extdta.drain(..consumed));
                    black_box(extdta.len())
                },
                BatchSize::SmallInput,
            );
        });
        queue_group.bench_function(BenchmarkId::new("pop_front", consumed), |b| {
            b.iter_batched(
                || queue.clone(),
                |mut extdta| {
                    for _ in 0..consumed {
                        extdta.pop_front();
                    }
                    black_box(extdta.len())
                },
                BatchSize::SmallInput,
            );
        });
    }
    queue_group.finish();
}

fn bench_ddm_and_dss(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let mut group = c.benchmark_group("ddm_dss");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(250));
    for size in [64usize, 1024, 65_536, 1_048_576, 16_777_216] {
        let source = vec![0x6b; size];
        let ddm_object = ddm::pack_ddm_object(cp::SQLDTA, &source);
        let reply_frame = dss::encode_reply_dss(&ddm_object, false, 1);
        let _ = dss::encode_request_dss(&ddm_object, 1, false, true).unwrap();
        let label = size.to_string();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(BenchmarkId::new("encode_request_dss", &label), |b| {
            b.iter(|| {
                black_box(dss::encode_request_dss(black_box(&ddm_object), 1, false, true).unwrap())
            });
        });
        group.bench_function(BenchmarkId::new("read_ddm_object", &label), |b| {
            b.iter(|| black_box(dss::read_ddm_object(black_box(&ddm_object)).unwrap()));
        });
        group.bench_function(BenchmarkId::new("parse_ddm_parameters", &label), |b| {
            b.iter(|| black_box(ddm::parse_ddm_parameters(black_box(&ddm_object)).unwrap()));
        });
        group.bench_function(BenchmarkId::new("read_dss", &label), |b| {
            b.to_async(&runtime).iter(|| async {
                let mut input = Cursor::new(black_box(&reply_frame));
                black_box(dss::read_dss(&mut input).await.unwrap())
            });
        });
    }

    for count in [1usize, 10, 100] {
        let objects = (0..count)
            .flat_map(|index| ddm::pack_u16(cp::SVRCOD, index as u16))
            .collect::<Vec<_>>();
        group.throughput(Throughput::Elements(count as u64));
        group.bench_function(BenchmarkId::new("parse_small_parameter_list", count), |b| {
            b.iter(|| black_box(ddm::parse_ddm_parameters(black_box(&objects)).unwrap()));
        });
        group.bench_function(BenchmarkId::new("visit_small_parameter_list", count), |b| {
            b.iter(|| {
                let mut visited = 0u16;
                ddm::visit_ddm_parameters(black_box(&objects), |object| {
                    visited ^= object.codepoint.0;
                    black_box(object.payload);
                })
                .unwrap();
                black_box(visited)
            });
        });
        group.bench_function(BenchmarkId::new("find_small_parameter_list", count), |b| {
            b.iter(|| black_box(ddm::find_ddm_parameter(black_box(&objects), cp::SVRCOD).unwrap()));
        });
    }

    let mut descriptor = Vec::new();
    for start in (0..100usize).step_by(84) {
        let count = (100 - start).min(84);
        descriptor.push(((count + 1) * 3) as u8);
        descriptor.push(if start == 0 { 0x76 } else { 0x7f });
        descriptor.push(if start == 0 { 0xd0 } else { 0 });
        for index in start..start + count {
            descriptor.extend_from_slice(&[index as u8, 0, 0]);
        }
    }
    group.bench_function("parse_query_descriptor_100_columns", |b| {
        b.iter(|| black_box(ddm::parse_query_descriptor(black_box(&descriptor)).unwrap()));
    });
    group.finish();

    let mut write_group = c.benchmark_group("dss_request_write");
    write_group.sample_size(30);
    write_group.warm_up_time(Duration::from_millis(500));
    write_group.measurement_time(Duration::from_secs(1));
    for size in [64usize, 1024, 65_536, 1_048_576, 16_777_216] {
        let source = vec![0x6b; size];
        let ddm_object = ddm::pack_ddm_object(cp::SQLDTA, &source);
        write_group.bench_function(BenchmarkId::new("materialize_then_write", size), |b| {
            b.to_async(&runtime).iter(|| async {
                let mut writer = CountingWriter::default();
                let (framed, _) =
                    dss::encode_request_dss(black_box(&ddm_object), 1, false, true).unwrap();
                writer.write_all(black_box(&framed)).await.unwrap();
                black_box((writer.bytes_written, writer.write_calls))
            });
        });
        write_group.bench_function(BenchmarkId::new("vectored_no_frame_copy", size), |b| {
            b.to_async(&runtime).iter(|| async {
                let mut writer = CountingWriter::default();
                dss::benchmark_write_request_dss(&mut writer, black_box(&ddm_object))
                    .await
                    .unwrap();
                black_box((writer.bytes_written, writer.write_calls))
            });
        });
    }

    let execute = ddm::pack_ddm_object(cp::EXCSQLSTT, &[0x31; 24]);
    let sql_dta = ddm::pack_ddm_object(cp::SQLDTA, &[0x5a; 1024]);
    let commit = ddm::pack_commit();
    write_group.bench_function("3_packet_materialize_then_write", |b| {
        b.to_async(&runtime).iter(|| async {
            let mut writer = CountingWriter::default();
            let (framed_execute, _) = dss::encode_request_dss(&execute, 1, true, false).unwrap();
            let (framed_data, _) = dss::encode_request_dss(&sql_dta, 1, false, false).unwrap();
            let (framed_commit, _) = dss::encode_request_dss(&commit, 1, false, true).unwrap();
            writer.write_all(&framed_execute).await.unwrap();
            writer.write_all(&framed_data).await.unwrap();
            writer.write_all(&framed_commit).await.unwrap();
            black_box((writer.bytes_written, writer.write_calls))
        });
    });
    write_group.bench_function("3_packet_vectored_chain", |b| {
        b.to_async(&runtime).iter(|| async {
            let mut writer = CountingWriter::default();
            dss::benchmark_write_three_request_chain(
                &mut writer,
                black_box(&execute),
                black_box(&sql_dta),
                black_box(&commit),
            )
            .await
            .unwrap();
            black_box((writer.bytes_written, writer.write_calls))
        });
    });
    write_group.finish();

    let mut lob_group = c.benchmark_group("dss_lob_stream_to_sink");
    lob_group.sample_size(10);
    lob_group.warm_up_time(Duration::from_millis(100));
    lob_group.measurement_time(Duration::from_millis(250));
    for size in [1_048_576usize, 16_777_216] {
        let source = vec![0x6b; size];
        lob_group.throughput(Throughput::Bytes(size as u64));
        lob_group.bench_function(BenchmarkId::new("extdta", size), |b| {
            b.to_async(&runtime).iter(|| async {
                let mut reader = Cursor::new(black_box(&source[..]));
                let mut sink = tokio::io::sink();
                dss::write_external_blob_dss(&mut sink, &mut reader, size as u64, 1, false, true)
                    .await
                    .unwrap();
                black_box(())
            });
        });
    }
    lob_group.finish();
}

criterion_group!(
    hot_paths,
    bench_value_encoding,
    bench_row_decoding,
    bench_ddm_and_dss
);
criterion_main!(hot_paths);
