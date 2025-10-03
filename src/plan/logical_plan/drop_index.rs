#[derive(Debug, Clone)]
pub struct DropIndex {
    pub name: String,
    pub schema: Option<String>,
    pub catalog: Option<String>,
    pub if_exists: bool,
}

impl std::fmt::Display for DropIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let qualified = match (&self.catalog, &self.schema) {
            (Some(catalog), Some(schema)) => format!("{catalog}.{schema}.{}", self.name),
            (None, Some(schema)) => format!("{schema}.{}", self.name),
            _ => self.name.clone(),
        };
        if self.if_exists {
            write!(f, "DropIndex IF EXISTS: {qualified}")
        } else {
            write!(f, "DropIndex: {qualified}")
        }
    }
}
