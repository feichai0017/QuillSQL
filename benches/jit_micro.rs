use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion::arrow::array::{Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use quill_sql::database::{Database, DatabaseOptions};
#[cfg(feature = "jit-mlir")]
use quill_sql::jit::DecimalFilterSumInput;
use quill_sql::jit::{
    FilterProjectKernel, JitBinaryOp, JitExpr, JitProjection, JitScalar, JitType, KernelBackend,
    MlirBackend, PipelineIr, PipelineOp,
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn sum_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("discount", DataType::Float64, false),
    ]))
}

fn predicate() -> JitExpr {
    JitExpr::Binary {
        op: JitBinaryOp::Gt,
        left: Box::new(JitExpr::Column {
            index: 1,
            name: "v".to_string(),
            ty: JitType::Int64,
            nullable: false,
        }),
        right: Box::new(JitExpr::Literal(JitScalar::Int64(500))),
        ty: JitType::Bool,
        nullable: false,
    }
}

fn projections() -> Vec<JitProjection> {
    vec![JitProjection::new(
        JitExpr::Binary {
            op: JitBinaryOp::Add,
            left: Box::new(JitExpr::Column {
                index: 0,
                name: "id".to_string(),
                ty: JitType::Int64,
                nullable: false,
            }),
            right: Box::new(JitExpr::Literal(JitScalar::Int64(1))),
            ty: JitType::Int64,
            nullable: false,
        },
        "next_id",
    )]
}

fn benchmark_database() -> Database {
    Database::new(DatabaseOptions {
        debug_trace: false,
        ..Default::default()
    })
    .expect("database")
}

#[cfg(feature = "jit-mlir")]
fn measure() -> JitExpr {
    JitExpr::Binary {
        op: JitBinaryOp::Mul,
        left: Box::new(JitExpr::Column {
            index: 0,
            name: "price".to_string(),
            ty: JitType::Float64,
            nullable: false,
        }),
        right: Box::new(JitExpr::Column {
            index: 1,
            name: "discount".to_string(),
            ty: JitType::Float64,
            nullable: false,
        }),
        ty: JitType::Float64,
        nullable: false,
    }
}

#[cfg(feature = "jit-mlir")]
fn q6_decimal_predicate() -> JitExpr {
    and(
        and(
            compare(JitBinaryOp::GtEq, date_col(0, "shipdate"), date_lit(10)),
            compare(JitBinaryOp::Lt, date_col(0, "shipdate"), date_lit(20)),
        ),
        and(
            and(
                compare(
                    JitBinaryOp::GtEq,
                    decimal_col(2, "discount", 2),
                    decimal_lit(5, 15, 2),
                ),
                compare(
                    JitBinaryOp::LtEq,
                    decimal_col(2, "discount", 2),
                    decimal_lit(7, 15, 2),
                ),
            ),
            compare(
                JitBinaryOp::Lt,
                decimal_col(3, "quantity", 2),
                decimal_lit(2_400, 15, 2),
            ),
        ),
    )
}

#[cfg(feature = "jit-mlir")]
fn q6_decimal_measure() -> JitExpr {
    JitExpr::Binary {
        op: JitBinaryOp::Mul,
        left: Box::new(decimal_col(1, "extendedprice", 2)),
        right: Box::new(decimal_col(2, "discount", 2)),
        ty: JitType::Decimal128 {
            precision: 38,
            scale: 4,
        },
        nullable: false,
    }
}

#[cfg(feature = "jit-mlir")]
fn and(left: JitExpr, right: JitExpr) -> JitExpr {
    JitExpr::Binary {
        op: JitBinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
        ty: JitType::Bool,
        nullable: false,
    }
}

#[cfg(feature = "jit-mlir")]
fn compare(op: JitBinaryOp, left: JitExpr, right: JitExpr) -> JitExpr {
    JitExpr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
        ty: JitType::Bool,
        nullable: false,
    }
}

#[cfg(feature = "jit-mlir")]
fn date_col(index: usize, name: &str) -> JitExpr {
    JitExpr::Column {
        index,
        name: name.to_string(),
        ty: JitType::Date32,
        nullable: false,
    }
}

#[cfg(feature = "jit-mlir")]
fn date_lit(value: i32) -> JitExpr {
    JitExpr::Literal(JitScalar::Date32(value))
}

#[cfg(feature = "jit-mlir")]
fn decimal_col(index: usize, name: &str, scale: i8) -> JitExpr {
    JitExpr::Column {
        index,
        name: name.to_string(),
        ty: JitType::Decimal128 {
            precision: 15,
            scale,
        },
        nullable: false,
    }
}

#[cfg(feature = "jit-mlir")]
fn decimal_lit(value: i128, precision: u8, scale: i8) -> JitExpr {
    JitExpr::Literal(JitScalar::Decimal128 {
        value,
        precision,
        scale,
    })
}

fn bench_jit_ir_and_mlir(c: &mut Criterion) {
    let input_schema = schema();
    let predicate = predicate();
    let projections = projections();
    let backend = MlirBackend::new();

    c.bench_function("jit_ir/fuse_filter_project", |b| {
        b.iter(|| {
            let pipeline = PipelineIr::new(vec![
                PipelineOp::Filter(black_box(predicate.clone())),
                PipelineOp::Projection(black_box(projections.clone())),
            ]);
            black_box(pipeline.first_kernel())
        });
    });

    c.bench_function("mlir/compile_filter", |b| {
        b.iter(|| {
            black_box(
                backend
                    .compile_filter(black_box(Arc::clone(&input_schema)), black_box(&predicate))
                    .expect("compile filter"),
            )
        });
    });

    c.bench_function("mlir/compile_filter_project", |b| {
        b.iter(|| {
            black_box(
                backend
                    .compile_filter_project(
                        black_box(Arc::clone(&input_schema)),
                        black_box(&predicate),
                        black_box(&projections),
                    )
                    .expect("compile filter project"),
            )
        });
    });

    #[cfg(feature = "jit-mlir")]
    c.bench_function("mlir_compiled/compile_i64_filter", |b| {
        b.iter(|| {
            black_box(
                backend
                    .compile_i64_filter(black_box(&predicate))
                    .expect("compile i64 filter"),
            )
        });
    });

    #[cfg(feature = "jit-mlir")]
    c.bench_function("mlir_compiled/compile_i64_filter_project", |b| {
        b.iter(|| {
            black_box(
                backend
                    .compile_i64_filter_project(black_box(&predicate), black_box(&projections))
                    .expect("compile i64 filter-project"),
            )
        });
    });

    #[cfg(feature = "jit-mlir")]
    c.bench_function("mlir_compiled/compile_f64_filter_sum", |b| {
        let measure = measure();
        b.iter(|| {
            black_box(
                backend
                    .compile_f64_filter_sum(black_box(&predicate), black_box(&measure))
                    .expect("compile f64 filter-sum"),
            )
        });
    });

    #[cfg(feature = "jit-mlir")]
    c.bench_function("mlir_compiled/compile_decimal_filter_sum", |b| {
        let predicate = q6_decimal_predicate();
        let measure = q6_decimal_measure();
        b.iter(|| {
            black_box(
                backend
                    .compile_decimal_filter_sum(black_box(&predicate), black_box(&measure))
                    .expect("compile decimal filter-sum"),
            )
        });
    });
}

fn bench_datafusion_filter_project(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let db = benchmark_database();
    let input_schema = schema();
    let row_count = 65_536_i64;
    let ids = (0..row_count).collect::<Vec<_>>();
    let values = (0..row_count)
        .map(|value| value % 1_000)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("record batch");
    db.register_batches("t", input_schema, vec![batch])
        .expect("register table");

    runtime
        .block_on(db.run("select id + 1 as next_id from t where v > 500"))
        .expect("warmup");

    c.bench_function("datafusion/sql_filter_project_64k", |b| {
        b.iter(|| {
            black_box(
                runtime
                    .block_on(db.run(black_box("select id + 1 as next_id from t where v > 500")))
                    .expect("query"),
            )
        });
    });
}

fn bench_datafusion_filter_sum(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let db = benchmark_database();
    let input_schema = sum_schema();
    let row_count = 65_536_i64;
    let values = (0..row_count)
        .map(|value| value % 1_000)
        .collect::<Vec<_>>();
    let prices = (0..row_count)
        .map(|value| 100.0 + (value % 10) as f64)
        .collect::<Vec<_>>();
    let discounts = (0..row_count)
        .map(|value| 0.01 * ((value % 7) as f64))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![
            Arc::new(Int64Array::from(values)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(discounts)),
        ],
    )
    .expect("record batch");
    db.register_batches("t", input_schema, vec![batch])
        .expect("register table");

    runtime
        .block_on(db.run("select sum(price * discount) from t where v > 500"))
        .expect("warmup");

    c.bench_function("datafusion/sql_filter_sum_64k", |b| {
        b.iter(|| {
            black_box(
                runtime
                    .block_on(db.run(black_box(
                        "select sum(price * discount) from t where v > 500",
                    )))
                    .expect("query"),
            )
        });
    });
}

fn bench_quill_filter_project_kernel(c: &mut Criterion) {
    let input_schema = schema();
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "next_id",
        DataType::Int64,
        false,
    )]));
    let row_count = 65_536_i64;
    let ids = (0..row_count).collect::<Vec<_>>();
    let values = (0..row_count)
        .map(|value| value % 1_000)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("record batch");
    let kernel =
        FilterProjectKernel::try_new(predicate(), projections(), output_schema).expect("kernel");

    c.bench_function("quill_kernel/filter_project_64k", |b| {
        b.iter(|| black_box(kernel.execute(black_box(&batch)).expect("execute kernel")));
    });
}

#[cfg(feature = "jit-mlir")]
fn bench_compiled_i64_filter_kernel(c: &mut Criterion) {
    let row_count = 65_536_i64;
    let values = (0..row_count)
        .map(|value| value % 1_000)
        .collect::<Vec<_>>();
    let mut output = vec![0_u8; values.len()];
    let kernel = MlirBackend::new()
        .compile_i64_filter(&predicate())
        .expect("compiled i64 filter");

    c.bench_function("mlir_compiled/filter_64k", |b| {
        b.iter(|| {
            kernel
                .invoke(black_box(&values), black_box(&mut output))
                .expect("execute compiled filter");
            black_box(&output);
        });
    });
}

#[cfg(feature = "jit-mlir")]
fn bench_compiled_i64_filter_project_kernel(c: &mut Criterion) {
    let row_count = 65_536_i64;
    let ids = (0..row_count).collect::<Vec<_>>();
    let values = (0..row_count)
        .map(|value| value % 1_000)
        .collect::<Vec<_>>();
    let mut output = vec![0_i64; values.len()];
    let kernel = MlirBackend::new()
        .compile_i64_filter_project(&predicate(), &projections())
        .expect("compiled i64 filter-project");

    c.bench_function("mlir_compiled/filter_project_64k", |b| {
        b.iter(|| {
            let output_len = kernel
                .invoke(black_box(&values), black_box(&ids), black_box(&mut output))
                .expect("execute compiled filter-project");
            black_box(output_len);
            black_box(&output[..output_len]);
        });
    });
}

#[cfg(feature = "jit-mlir")]
fn bench_compiled_f64_filter_sum_kernel(c: &mut Criterion) {
    let row_count = 65_536_i64;
    let predicate_values = (0..row_count)
        .map(|value| value % 1_000)
        .collect::<Vec<_>>();
    let prices = (0..row_count)
        .map(|value| 100.0 + (value % 10) as f64)
        .collect::<Vec<_>>();
    let discounts = (0..row_count)
        .map(|value| 0.01 * ((value % 7) as f64))
        .collect::<Vec<_>>();
    let kernel = MlirBackend::new()
        .compile_f64_filter_sum(&predicate(), &measure())
        .expect("compiled f64 filter-sum");

    c.bench_function("mlir_compiled/filter_sum_64k", |b| {
        b.iter(|| {
            black_box(
                kernel
                    .invoke(
                        black_box(&predicate_values),
                        black_box(&prices),
                        black_box(&discounts),
                    )
                    .expect("execute compiled filter-sum"),
            );
        });
    });
}

#[cfg(feature = "jit-mlir")]
fn bench_compiled_decimal_filter_sum_kernel(c: &mut Criterion) {
    let row_count = 65_536_i32;
    let shipdates = (0..row_count)
        .map(|value| 10 + (value % 12))
        .collect::<Vec<_>>();
    let prices = (0..row_count)
        .map(|value| 10_000_i128 + i128::from(value % 1_000))
        .collect::<Vec<_>>();
    let discounts = (0..row_count)
        .map(|value| 4_i128 + i128::from(value % 5))
        .collect::<Vec<_>>();
    let quantities = (0..row_count)
        .map(|value| 2_000_i128 + i128::from(value % 600))
        .collect::<Vec<_>>();
    let kernel = MlirBackend::new()
        .compile_decimal_filter_sum(&q6_decimal_predicate(), &q6_decimal_measure())
        .expect("compiled decimal filter-sum");

    c.bench_function("mlir_compiled/decimal_filter_sum_64k", |b| {
        b.iter(|| {
            black_box(
                kernel
                    .invoke(&[
                        DecimalFilterSumInput::Date32 {
                            index: 0,
                            values: black_box(shipdates.as_slice()),
                        },
                        DecimalFilterSumInput::Decimal128 {
                            index: 1,
                            values: black_box(prices.as_slice()),
                        },
                        DecimalFilterSumInput::Decimal128 {
                            index: 2,
                            values: black_box(discounts.as_slice()),
                        },
                        DecimalFilterSumInput::Decimal128 {
                            index: 3,
                            values: black_box(quantities.as_slice()),
                        },
                    ])
                    .map(|output| (output.sum, output.count))
                    .expect("execute compiled decimal filter-sum"),
            );
        });
    });
}

#[cfg(not(feature = "jit-mlir"))]
criterion_group!(
    benches,
    bench_jit_ir_and_mlir,
    bench_quill_filter_project_kernel,
    bench_datafusion_filter_project,
    bench_datafusion_filter_sum
);

#[cfg(feature = "jit-mlir")]
criterion_group!(
    benches,
    bench_jit_ir_and_mlir,
    bench_compiled_i64_filter_kernel,
    bench_compiled_i64_filter_project_kernel,
    bench_compiled_f64_filter_sum_kernel,
    bench_compiled_decimal_filter_sum_kernel,
    bench_quill_filter_project_kernel,
    bench_datafusion_filter_project,
    bench_datafusion_filter_sum
);
criterion_main!(benches);
