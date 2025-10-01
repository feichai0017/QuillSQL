#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableReference {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: String,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: String,
        /// The table name
        table: String,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: String,
        /// The schema containing the table
        schema: String,
        /// The table name
        table: String,
    },
}

impl TableReference {
    pub fn table(&self) -> &str {
        match self {
            Self::Full { table, .. } | Self::Partial { table, .. } | Self::Bare { table } => table,
        }
    }

    pub fn schema(&self) -> Option<&str> {
        match self {
            Self::Full { schema, .. } | Self::Partial { schema, .. } => Some(schema),
            _ => None,
        }
    }

    pub fn catalog(&self) -> Option<&str> {
        match self {
            Self::Full { catalog, .. } => Some(catalog),
            _ => None,
        }
    }

    pub fn resolved_eq(&self, other: &Self) -> bool {
        match self {
            TableReference::Bare { table } => table == other.table(),
            TableReference::Partial { schema, table } => {
                table == other.table() && other.schema().map_or(true, |s| s == schema)
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                table == other.table()
                    && other.schema().map_or(true, |s| s == schema)
                    && other.catalog().map_or(true, |c| c == catalog)
            }
        }
    }

    pub fn to_log_string(&self) -> String {
        match self {
            TableReference::Bare { table } => table.clone(),
            TableReference::Partial { schema, table } => format!("{schema}.{table}"),
            TableReference::Full {
                catalog,
                schema,
                table,
            } => format!("{catalog}.{schema}.{table}"),
        }
    }
}

impl std::fmt::Display for TableReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableReference::Bare { table } => write!(f, "{table}"),
            TableReference::Partial { schema, table } => {
                write!(f, "{schema}.{table}")
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => write!(f, "{catalog}.{schema}.{table}"),
        }
    }
}
