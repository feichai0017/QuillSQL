use std::fmt::Write;

use crate::jit::{
    JitExpr, JitProjection, PipelineIr, PipelineKind, PipelineOp, PipelineSink, PipelineSource,
};

#[derive(Debug, Clone, PartialEq)]
pub struct QuillDialectModule {
    pub symbol: String,
    pub kind: PipelineKind,
    pub source: QuillDialectSource,
    pub ops: Vec<QuillDialectOp>,
    pub sink: QuillDialectSink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuillDialectSource {
    DataFusionBatch,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QuillDialectOp {
    Filter { predicate: JitExpr },
    Project { projections: Vec<JitProjection> },
    Limit { fetch: usize },
}

#[derive(Debug, Clone, PartialEq)]
pub enum QuillDialectSink {
    RecordBatch,
    PlainSum { measure: JitExpr },
}

impl QuillDialectModule {
    pub fn from_pipeline(symbol: impl Into<String>, pipeline: &PipelineIr) -> Self {
        let source = match &pipeline.source {
            PipelineSource::DataFusionInput => QuillDialectSource::DataFusionBatch,
        };
        let ops = pipeline
            .operators
            .iter()
            .map(|op| match op {
                PipelineOp::Filter(predicate) => QuillDialectOp::Filter {
                    predicate: predicate.clone(),
                },
                PipelineOp::Projection(projections) => QuillDialectOp::Project {
                    projections: projections.clone(),
                },
                PipelineOp::Limit(fetch) => QuillDialectOp::Limit { fetch: *fetch },
            })
            .collect();
        let sink = match &pipeline.sink {
            PipelineSink::RecordBatch => QuillDialectSink::RecordBatch,
            PipelineSink::Sum { measure } => QuillDialectSink::PlainSum {
                measure: measure.clone(),
            },
        };

        Self {
            symbol: symbol.into(),
            kind: sink.kind(),
            source,
            ops,
            sink,
        }
    }

    pub fn text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "module {{");
        let _ = writeln!(text, "  \"quill.pipeline\"() ({{ // @{}", self.symbol);
        let _ = writeln!(
            text,
            "    %batch0 = \"{}\"() : () -> !quill.batch",
            self.source.name()
        );

        let mut batch = "%batch0".to_string();
        let mut selection = None::<String>;
        let mut next_batch = 1_usize;
        let mut next_selection = 0_usize;

        for op in &self.ops {
            match op {
                QuillDialectOp::Filter { predicate } => {
                    let out = format!("%sel{next_selection}");
                    next_selection += 1;
                    let _ = writeln!(
                        text,
                        "    {out} = \"quill.exec.filter\"({batch}) {{ predicate = {} }} : (!quill.batch) -> !quill.selection",
                        string_attr(&predicate.to_string())
                    );
                    selection = Some(out);
                }
                QuillDialectOp::Project { projections } => {
                    let sel =
                        ensure_selection(&mut text, &batch, &mut selection, &mut next_selection);
                    let out = format!("%batch{next_batch}");
                    next_batch += 1;
                    let _ = writeln!(
                        text,
                        "    {out} = \"quill.exec.project\"({batch}, {sel}) {{ exprs = [{}] }} : (!quill.batch, !quill.selection) -> !quill.batch",
                        projections
                            .iter()
                            .map(project_attr)
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    batch = out;
                }
                QuillDialectOp::Limit { fetch } => {
                    let sel =
                        ensure_selection(&mut text, &batch, &mut selection, &mut next_selection);
                    let out = format!("%sel{next_selection}");
                    next_selection += 1;
                    let _ = writeln!(
                        text,
                        "    {out} = \"quill.exec.limit\"({sel}) {{ fetch = {fetch} : i64 }} : (!quill.selection) -> !quill.selection"
                    );
                    selection = Some(out);
                }
            }
        }

        self.append_sink(&mut text, &batch, selection.as_deref());
        let _ = writeln!(
            text,
            "  }}) {{ name = {}, kind = {} }} : () -> ()",
            string_attr(&self.symbol),
            string_attr(self.kind.name())
        );
        let _ = writeln!(text, "}}");
        text
    }

    fn append_sink(&self, text: &mut String, batch: &str, selection: Option<&str>) {
        match &self.sink {
            QuillDialectSink::RecordBatch => {
                if let Some(selection) = selection {
                    let _ = writeln!(
                        text,
                        "    \"quill.sink.record_batch\"({batch}, {selection}) : (!quill.batch, !quill.selection) -> ()"
                    );
                } else {
                    let _ = writeln!(
                        text,
                        "    \"quill.sink.record_batch\"({batch}) : (!quill.batch) -> ()"
                    );
                }
            }
            QuillDialectSink::PlainSum { measure } => {
                if let Some(selection) = selection {
                    let _ = writeln!(
                        text,
                        "    \"quill.sink.plain_sum\"({batch}, {selection}) {{ measure = {} }} : (!quill.batch, !quill.selection) -> !quill.scalar",
                        string_attr(&measure.to_string())
                    );
                } else {
                    let _ = writeln!(
                        text,
                        "    \"quill.sink.plain_sum\"({batch}) {{ measure = {} }} : (!quill.batch) -> !quill.scalar",
                        string_attr(&measure.to_string())
                    );
                }
            }
        }
    }
}

impl QuillDialectSource {
    fn name(self) -> &'static str {
        match self {
            Self::DataFusionBatch => "quill.source.datafusion_batch",
        }
    }
}

impl QuillDialectSink {
    fn kind(&self) -> PipelineKind {
        match self {
            Self::RecordBatch => PipelineKind::Record,
            Self::PlainSum { .. } => PipelineKind::Aggregate,
        }
    }
}

impl PipelineKind {
    fn name(self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::Aggregate => "aggregate",
        }
    }
}

fn ensure_selection(
    text: &mut String,
    batch: &str,
    selection: &mut Option<String>,
    next_selection: &mut usize,
) -> String {
    if let Some(selection) = selection {
        return selection.clone();
    }

    let out = format!("%sel{next_selection}");
    *next_selection += 1;
    let _ = writeln!(
        text,
        "    {out} = \"quill.selection.all\"({batch}) : (!quill.batch) -> !quill.selection"
    );
    *selection = Some(out.clone());
    out
}

fn project_attr(projection: &JitProjection) -> String {
    string_attr(&format!("{} = {}", projection.alias, projection.expr))
}

fn string_attr(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

#[cfg(test)]
mod tests {
    use crate::jit::{
        JitBinaryOp, JitExpr, JitProjection, JitScalar, JitType, PipelineIr, PipelineKind,
        PipelineOp, QuillDialectModule,
    };

    #[test]
    fn emits_record_pipeline_dialect_skeleton() {
        let predicate = i64_gt_ten();
        let projection = JitProjection::new(
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
        );
        let pipeline = PipelineIr::new(vec![
            PipelineOp::Filter(predicate),
            PipelineOp::Projection(vec![projection]),
        ]);

        let module = QuillDialectModule::from_pipeline("record0", &pipeline);
        let text = module.text();

        assert_eq!(module.kind, PipelineKind::Record);
        assert!(text.contains("\"quill.pipeline\""));
        assert!(text.contains("\"quill.source.datafusion_batch\""));
        assert!(text.contains("\"quill.exec.filter\""));
        assert!(text.contains("\"quill.exec.project\""));
        assert!(text.contains("\"quill.sink.record_batch\""));
    }

    #[test]
    fn emits_plain_sum_pipeline_dialect_skeleton() {
        let predicate = i64_gt_ten();
        let measure = JitExpr::Column {
            index: 1,
            name: "price".to_string(),
            ty: JitType::Float64,
            nullable: false,
        };
        let pipeline = PipelineIr::filter_sum(predicate, measure);

        let module = QuillDialectModule::from_pipeline("sum0", &pipeline);
        let text = module.text();

        assert_eq!(module.kind, PipelineKind::Aggregate);
        assert!(text.contains("\"quill.exec.filter\""));
        assert!(text.contains("\"quill.sink.plain_sum\""));
        assert!(text.contains("measure = \"col(1, price, f64, nullable=false)\""));
    }

    fn i64_gt_ten() -> JitExpr {
        JitExpr::Binary {
            op: JitBinaryOp::Gt,
            left: Box::new(JitExpr::Column {
                index: 0,
                name: "v".to_string(),
                ty: JitType::Int64,
                nullable: false,
            }),
            right: Box::new(JitExpr::Literal(JitScalar::Int64(10))),
            ty: JitType::Bool,
            nullable: false,
        }
    }
}
