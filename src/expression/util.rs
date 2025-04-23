use crate::catalog::SchemaRef;
use crate::expression::{Alias, Cast, ColumnExpr, Expr};
use crate::error::QuillSQLResult;

/// Convert an expression into Column expression
pub fn columnize_expr(e: &Expr, input_schema: &SchemaRef) -> QuillSQLResult<Expr> {
    match e {
        Expr::Column(_) => Ok(e.clone()),
        Expr::Alias(Alias { expr, name }) => Ok(Expr::Alias(Alias {
            expr: Box::new(columnize_expr(expr, input_schema)?),
            name: name.clone(),
        })),
        Expr::Cast(Cast { expr, data_type }) => Ok(Expr::Cast(Cast {
            expr: Box::new(columnize_expr(expr, input_schema)?),
            data_type: *data_type,
        })),
        _ => {
            let name = e.to_string();
            let idx = input_schema.index_of(None, name.as_str())?;
            let col = input_schema.column_with_index(idx)?;
            Ok(Expr::Column(ColumnExpr {
                relation: col.relation.clone(),
                name,
            }))
        }
    }
}
