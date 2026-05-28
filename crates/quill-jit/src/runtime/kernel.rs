use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use serde::Serialize;

use std::collections::BTreeMap;

use crate::{JitBinaryOp, JitExpr, JitProjection, JitResult, JitScalar, JitType, MlirColumn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum KernelKind {
    Filter,
    Projection,
    FilterProject,
    FilterSum,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PipelineSpec {
    Generic {
        kind: KernelKind,
    },
    RecordProject {
        columns: Vec<MlirColumn>,
        output_types: Vec<JitType>,
    },
    F64FilterSum {
        predicate_column: usize,
        predicate_op: JitBinaryOp,
        predicate_value: i64,
        measure_left_column: usize,
        measure_right_column: usize,
    },
    DecimalFilterSum {
        predicates: Vec<PredicateSpec>,
        measure_left_column: usize,
        measure_right_column: usize,
        output_scale: i8,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PredicateSpec {
    Date32 {
        column: usize,
        op: JitBinaryOp,
        value: i32,
    },
    Decimal128 {
        column: usize,
        op: JitBinaryOp,
        value: i128,
        scale: i8,
    },
    Int64 {
        column: usize,
        op: JitBinaryOp,
        value: i64,
    },
}

impl PipelineSpec {
    pub fn generic(kind: KernelKind) -> Self {
        Self::Generic { kind }
    }

    pub fn record_project(predicate: &JitExpr, projections: &[JitProjection]) -> Option<Self> {
        if predicate.ty() != JitType::Bool || projections.is_empty() {
            return None;
        }

        let mut columns = BTreeMap::new();
        collect_fixed_width_columns(predicate, &mut columns)?;
        let output_types = projections
            .iter()
            .map(|projection| {
                ensure_record_output_type(projection.expr.ty())?;
                collect_fixed_width_columns(&projection.expr, &mut columns)?;
                Some(projection.expr.ty())
            })
            .collect::<Option<Vec<_>>>()?;

        Some(Self::RecordProject {
            columns: columns
                .into_iter()
                .map(|(index, ty)| MlirColumn { index, ty })
                .collect(),
            output_types,
        })
    }

    pub fn filter_sum(predicate: &JitExpr, measure: &JitExpr) -> Option<Self> {
        if let (Some((predicate_column, predicate_op, predicate_value)), Some((left, right))) =
            (parse_i64_compare(predicate), parse_f64_mul(measure))
        {
            return Some(Self::F64FilterSum {
                predicate_column,
                predicate_op,
                predicate_value,
                measure_left_column: left,
                measure_right_column: right,
            });
        }

        let predicates = parse_fixed_predicates(predicate)?;
        let (left, right, scale) = parse_decimal_mul(measure)?;
        Some(Self::DecimalFilterSum {
            predicates,
            measure_left_column: left,
            measure_right_column: right,
            output_scale: scale,
        })
    }

    pub fn kind(&self) -> KernelKind {
        match self {
            Self::Generic { kind } => *kind,
            Self::RecordProject { .. } => KernelKind::FilterProject,
            Self::F64FilterSum { .. } | Self::DecimalFilterSum { .. } => KernelKind::FilterSum,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Generic { kind } => kind.name(),
            Self::RecordProject { .. } => "record_project",
            Self::F64FilterSum { .. } => "f64_filter_sum",
            Self::DecimalFilterSum { .. } => "decimal_filter_sum",
        }
    }
}

impl KernelKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Projection => "projection",
            Self::FilterProject => "filter_project",
            Self::FilterSum => "filter_sum",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompiledKernel {
    pub id: String,
    pub kind: KernelKind,
    pub spec: PipelineSpec,
    pub backend: String,
    pub executable: bool,
}

impl CompiledKernel {
    pub fn new(
        id: impl Into<String>,
        kind: KernelKind,
        backend: impl Into<String>,
        executable: bool,
    ) -> Self {
        Self {
            id: id.into(),
            kind,
            spec: PipelineSpec::generic(kind),
            backend: backend.into(),
            executable,
        }
    }

    pub fn with_spec(
        id: impl Into<String>,
        spec: PipelineSpec,
        backend: impl Into<String>,
        executable: bool,
    ) -> Self {
        let kind = spec.kind();
        Self {
            id: id.into(),
            kind,
            spec,
            backend: backend.into(),
            executable,
        }
    }
}

pub trait KernelBackend: Send + Sync {
    fn name(&self) -> &str;

    fn compile_filter(
        &self,
        input_schema: ArrowSchemaRef,
        predicate: &JitExpr,
    ) -> JitResult<CompiledKernel>;

    fn compile_projection(
        &self,
        input_schema: ArrowSchemaRef,
        projections: &[JitProjection],
    ) -> JitResult<CompiledKernel>;

    fn compile_filter_project(
        &self,
        input_schema: ArrowSchemaRef,
        predicate: &JitExpr,
        projections: &[JitProjection],
    ) -> JitResult<CompiledKernel>;
}

fn collect_fixed_width_columns(
    expr: &JitExpr,
    columns: &mut BTreeMap<usize, JitType>,
) -> Option<()> {
    match expr {
        JitExpr::Column { index, ty, .. } => {
            ensure_fixed_width_type(*ty)?;
            match columns.get(index) {
                Some(existing) if *existing != *ty => return None,
                Some(_) => {}
                None => {
                    columns.insert(*index, *ty);
                }
            }
            Some(())
        }
        JitExpr::Literal(_) => Some(()),
        JitExpr::Binary { left, right, .. } => {
            collect_fixed_width_columns(left, columns)?;
            collect_fixed_width_columns(right, columns)
        }
        JitExpr::IsNull(_) => None,
    }
}

fn ensure_fixed_width_type(ty: JitType) -> Option<()> {
    match ty {
        JitType::Date32 | JitType::Int64 | JitType::Float64 | JitType::Decimal128 { .. } => {
            Some(())
        }
        JitType::Bool | JitType::Int32 => None,
    }
}

fn ensure_record_output_type(ty: JitType) -> Option<()> {
    match ty {
        JitType::Date32 | JitType::Int64 | JitType::Float64 | JitType::Decimal128 { .. } => {
            Some(())
        }
        JitType::Bool | JitType::Int32 => None,
    }
}

fn parse_i64_compare(expr: &JitExpr) -> Option<(usize, JitBinaryOp, i64)> {
    let JitExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    if !is_compare_op(*op) {
        return None;
    }

    if let Some((column, threshold)) = parse_i64_column_literal(left, right) {
        return Some((column, *op, threshold));
    }
    if let Some((column, threshold)) = parse_i64_column_literal(right, left) {
        return Some((column, reverse_compare_op(*op), threshold));
    }
    None
}

fn parse_i64_column_literal(column: &JitExpr, literal: &JitExpr) -> Option<(usize, i64)> {
    let JitExpr::Column {
        index,
        ty: JitType::Int64,
        ..
    } = column
    else {
        return None;
    };
    let JitExpr::Literal(JitScalar::Int64(value)) = literal else {
        return None;
    };
    Some((*index, *value))
}

fn parse_fixed_predicates(expr: &JitExpr) -> Option<Vec<PredicateSpec>> {
    let mut predicates = Vec::new();
    collect_fixed_predicates(expr, &mut predicates)?;
    Some(predicates)
}

fn collect_fixed_predicates(expr: &JitExpr, predicates: &mut Vec<PredicateSpec>) -> Option<()> {
    if let JitExpr::Binary {
        op: JitBinaryOp::And,
        left,
        right,
        ..
    } = expr
    {
        collect_fixed_predicates(left, predicates)?;
        collect_fixed_predicates(right, predicates)?;
        return Some(());
    }

    predicates.push(parse_fixed_predicate(expr)?);
    Some(())
}

fn parse_fixed_predicate(expr: &JitExpr) -> Option<PredicateSpec> {
    let JitExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    if !is_compare_op(*op) {
        return None;
    }

    if let Some(predicate) = parse_fixed_column_literal(left, *op, right) {
        return Some(predicate);
    }
    parse_fixed_column_literal(right, reverse_compare_op(*op), left)
}

fn parse_fixed_column_literal(
    column: &JitExpr,
    op: JitBinaryOp,
    literal: &JitExpr,
) -> Option<PredicateSpec> {
    match (column, literal) {
        (
            JitExpr::Column {
                index,
                ty: JitType::Date32,
                ..
            },
            JitExpr::Literal(JitScalar::Date32(value)),
        ) => Some(PredicateSpec::Date32 {
            column: *index,
            op,
            value: *value,
        }),
        (
            JitExpr::Column {
                index,
                ty: JitType::Decimal128 { scale, .. },
                ..
            },
            JitExpr::Literal(JitScalar::Decimal128 {
                value,
                scale: literal_scale,
                ..
            }),
        ) if scale == literal_scale => Some(PredicateSpec::Decimal128 {
            column: *index,
            op,
            value: *value,
            scale: *scale,
        }),
        (
            JitExpr::Column {
                index,
                ty: JitType::Int64,
                ..
            },
            JitExpr::Literal(JitScalar::Int64(value)),
        ) => Some(PredicateSpec::Int64 {
            column: *index,
            op,
            value: *value,
        }),
        _ => None,
    }
}

fn parse_f64_mul(expr: &JitExpr) -> Option<(usize, usize)> {
    let JitExpr::Binary {
        op: JitBinaryOp::Mul,
        left,
        right,
        ..
    } = expr
    else {
        return None;
    };
    Some((parse_f64_column(left)?, parse_f64_column(right)?))
}

fn parse_f64_column(expr: &JitExpr) -> Option<usize> {
    let JitExpr::Column {
        index,
        ty: JitType::Float64,
        ..
    } = expr
    else {
        return None;
    };
    Some(*index)
}

fn parse_decimal_mul(expr: &JitExpr) -> Option<(usize, usize, i8)> {
    let JitExpr::Binary {
        op: JitBinaryOp::Mul,
        left,
        right,
        ty: JitType::Decimal128 { scale, .. },
        ..
    } = expr
    else {
        return None;
    };
    Some((
        parse_decimal_column(left)?,
        parse_decimal_column(right)?,
        *scale,
    ))
}

fn parse_decimal_column(expr: &JitExpr) -> Option<usize> {
    let JitExpr::Column {
        index,
        ty: JitType::Decimal128 { .. },
        ..
    } = expr
    else {
        return None;
    };
    Some(*index)
}

fn is_compare_op(op: JitBinaryOp) -> bool {
    matches!(
        op,
        JitBinaryOp::Eq
            | JitBinaryOp::NotEq
            | JitBinaryOp::Lt
            | JitBinaryOp::LtEq
            | JitBinaryOp::Gt
            | JitBinaryOp::GtEq
    )
}

fn reverse_compare_op(op: JitBinaryOp) -> JitBinaryOp {
    match op {
        JitBinaryOp::Lt => JitBinaryOp::Gt,
        JitBinaryOp::LtEq => JitBinaryOp::GtEq,
        JitBinaryOp::Gt => JitBinaryOp::Lt,
        JitBinaryOp::GtEq => JitBinaryOp::LtEq,
        JitBinaryOp::Eq | JitBinaryOp::NotEq => op,
        _ => unreachable!("non-comparison operator cannot be reversed"),
    }
}
