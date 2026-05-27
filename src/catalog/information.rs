use crate::catalog::{Catalog, CatalogSchema, DEFAULT_SCHEMA_NAME};
use crate::error::QuillSQLResult;

pub static INFORMATION_SCHEMA_NAME: &str = "information_schema";

pub fn load_catalog_data(catalog: &mut Catalog) -> QuillSQLResult<()> {
    create_default_schema_if_not_exists(catalog)?;
    load_descriptor_tables(catalog)?;
    load_descriptor_indexes(catalog)
}

fn create_default_schema_if_not_exists(catalog: &mut Catalog) -> QuillSQLResult<()> {
    if !catalog.schemas.contains_key(DEFAULT_SCHEMA_NAME) {
        catalog.create_schema(DEFAULT_SCHEMA_NAME)?;
    }
    Ok(())
}

fn load_descriptor_tables(catalog: &mut Catalog) -> QuillSQLResult<()> {
    for descriptor in catalog.holt_store.table_descriptors()? {
        if descriptor.table_ref.schema() == Some(INFORMATION_SCHEMA_NAME) {
            continue;
        }
        if catalog.try_table_schema(&descriptor.table_ref).is_some() {
            continue;
        }
        let schema_name = descriptor
            .table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        if !catalog.schemas.contains_key(&schema_name) {
            catalog.load_schema(schema_name.clone(), CatalogSchema::new(schema_name));
        }
        catalog.load_table(
            descriptor.table_ref,
            descriptor.table_name,
            descriptor.schema,
            descriptor.table_id,
        )?;
    }
    Ok(())
}

fn load_descriptor_indexes(catalog: &mut Catalog) -> QuillSQLResult<()> {
    for descriptor in catalog.holt_store.index_descriptors()? {
        if descriptor.table_ref.schema() == Some(INFORMATION_SCHEMA_NAME) {
            continue;
        }
        if catalog.try_table_schema(&descriptor.table_ref).is_none() {
            continue;
        }
        if catalog
            .table_indexes(&descriptor.table_ref)
            .map(|indexes| {
                indexes
                    .iter()
                    .any(|index| index.name == descriptor.index_name)
            })
            .unwrap_or(false)
        {
            continue;
        }
        catalog.load_index(
            descriptor.table_ref,
            descriptor.index_name,
            descriptor.key_schema,
            descriptor.index_id,
        )?;
    }
    Ok(())
}
