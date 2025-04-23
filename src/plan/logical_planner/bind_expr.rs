use crate::expression::{AggregateFunction, BinaryExpr, ColumnExpr, Expr, Literal};
use crate::function::AggregateFunctionKind;
use crate::plan::LogicalPlanner;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;
use crate::sql::ast;

impl LogicalPlanner<'_> {
    pub fn bind_expr(&self, sql: &ast::Expr) -> QuillSQLResult<Expr> {
        match sql {
            ast::Expr::Identifier(ident) => Ok(Expr::Column(ColumnExpr {
                relation: None,
                name: ident.value.clone(),
            })),
            ast::Expr::BinaryOp { left, op, right } => {
                let left = Box::new(self.bind_expr(left)?);
                let right = Box::new(self.bind_expr(right)?);
                Ok(Expr::Binary(BinaryExpr {
                    left,
                    op: op.try_into()?,
                    right,
                }))
            }
            ast::Expr::Value(value) => self.bind_value(value),
            ast::Expr::CompoundIdentifier(idents) => match idents.as_slice() {
                [col] => Ok(Expr::Column(ColumnExpr {
                    relation: None,
                    name: col.value.clone(),
                })),
                [table, col] => Ok(Expr::Column(ColumnExpr {
                    relation: Some(TableReference::bare(table.value.clone())),
                    name: col.value.clone(),
                })),
                [schema, table, col] => Ok(Expr::Column(ColumnExpr {
                    relation: Some(TableReference::partial(
                        schema.value.clone(),
                        table.value.clone(),
                    )),
                    name: col.value.clone(),
                })),
                [catalog, schema, table, col] => Ok(Expr::Column(ColumnExpr {
                    relation: Some(TableReference::full(
                        catalog.value.clone(),
                        schema.value.clone(),
                        table.value.clone(),
                    )),
                    name: col.value.clone(),
                })),
                _ => Err(QuillSQLError::NotSupport(format!(
                    "sqlparser expr CompoundIdentifier has more than 4 identifiers: {:?}",
                    idents
                ))),
            },
            ast::Expr::Function(function) => self.bind_function(function),
            _ => Err(QuillSQLError::NotSupport(format!(
                "sqlparser expr {} not supported",
                sql
            ))),
        }
    }

    pub fn bind_value(&self, value: &ast::Value) -> QuillSQLResult<Expr> {
        match value {
            ast::Value::Number(s, _) => {
                if let Ok(num) = s.parse::<i64>() {
                    return Ok(Expr::Literal(Literal { value: num.into() }));
                }
                if let Ok(num) = s.parse::<f64>() {
                    return Ok(Expr::Literal(Literal { value: num.into() }));
                }
                Err(QuillSQLError::Internal(
                    "Failed to parse sql number value".to_string(),
                ))
            }
            ast::Value::Boolean(b) => Ok(Expr::Literal(Literal { value: (*b).into() })),
            ast::Value::Null => Ok(Expr::Literal(Literal {
                value: ScalarValue::Int8(None),
            })),
            ast::Value::SingleQuotedString(s) => Ok(Expr::Literal(Literal {
                value: s.clone().into(),
            })),
            _ => Err(QuillSQLError::NotSupport(format!(
                "sqlparser value {} not supported",
                value
            ))),
        }
    }

    pub fn bind_function(&self, function: &ast::Function) -> QuillSQLResult<Expr> {
        let name = function.name.to_string();

        if let Some(func_kind) = AggregateFunctionKind::find(name.as_str()) {
            let args = function
                .args
                .iter()
                .map(|arg| self.bind_function_arg(arg))
                .collect::<QuillSQLResult<Vec<Expr>>>()?;
            return Ok(Expr::AggregateFunction(AggregateFunction {
                func_kind,
                args,
                distinct: function.distinct,
            }));
        }

        Err(QuillSQLError::Plan(format!(
            "The function {} is not supported",
            function
        )))
    }

    pub fn bind_function_arg(&self, arg: &ast::FunctionArg) -> QuillSQLResult<Expr> {
        match arg {
            ast::FunctionArg::Named {
                name: _,
                arg: sqlparser::ast::FunctionArgExpr::Expr(arg),
            } => self.bind_expr(arg),
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg)) => {
                self.bind_expr(arg)
            }
            _ => Err(QuillSQLError::Plan(format!(
                "The function arg {} is not supported",
                arg
            ))),
        }
    }
}
