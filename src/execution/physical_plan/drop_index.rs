use crate::catalog::{SchemaRef, EMPTY_SCHEMA_REF};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::storage::tuple::Tuple;
use crate::transaction::LockMode;

#[derive(Debug)]
pub struct PhysicalDropIndex {
    pub name: String,
    pub schema: Option<String>,
    pub catalog: Option<String>,
    pub if_exists: bool,
}

impl PhysicalDropIndex {
    pub fn new(
        name: String,
        schema: Option<String>,
        catalog: Option<String>,
        if_exists: bool,
    ) -> Self {
        Self {
            name,
            schema,
            catalog,
            if_exists,
        }
    }

    fn qualified_name(&self) -> String {
        match (&self.catalog, &self.schema) {
            (Some(catalog), Some(schema)) => format!("{catalog}.{schema}.{}", self.name),
            (None, Some(schema)) => format!("{schema}.{}", self.name),
            _ => self.name.clone(),
        }
    }
}

impl VolcanoExecutor for PhysicalDropIndex {
    fn init(&self, _context: &mut ExecutionContext) -> QuillSQLResult<()> {
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let owner =
            context.find_index_owner(self.catalog.as_deref(), self.schema.as_deref(), &self.name);

        let Some(table_ref) = owner else {
            if self.if_exists {
                return Ok(None);
            }
            return Err(QuillSQLError::Execution(format!(
                "index {} does not exist",
                self.qualified_name()
            )));
        };

        context.ensure_writable(&table_ref, "DROP INDEX")?;
        context.lock_table(table_ref.clone(), LockMode::Exclusive)?;

        let dropped = context.drop_index(&table_ref, &self.name)?;
        if !dropped && !self.if_exists {
            return Err(QuillSQLError::Execution(format!(
                "index {} does not exist",
                self.qualified_name()
            )));
        }

        Ok(None)
    }

    fn output_schema(&self) -> SchemaRef {
        EMPTY_SCHEMA_REF.clone()
    }
}

impl std::fmt::Display for PhysicalDropIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DropIndex: {}", self.qualified_name())
    }
}
