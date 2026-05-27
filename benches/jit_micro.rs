use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use quill_sql::database::Database;
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
}

fn bench_datafusion_filter_project(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let db = Database::new_temp().expect("database");
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

criterion_group!(
    benches,
    bench_jit_ir_and_mlir,
    bench_quill_filter_project_kernel,
    bench_datafusion_filter_project
);
criterion_main!(benches);
