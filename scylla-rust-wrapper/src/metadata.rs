use crate::argconv::*;
use crate::cass_column_types::CassColumnType;
use crate::cass_types::get_column_type_from_cql_type;
use crate::cass_types::CassDataType;
use crate::types::*;
use scylla::transport::topology::{ColumnKind, Table, UserDefinedType};
use std::collections::HashMap;
use std::os::raw::c_char;
use std::sync::Arc;
use std::sync::Weak;

pub struct CassSchemaMeta {
    pub keyspaces: HashMap<String, CassKeyspaceMeta>,
}

impl FFI for CassSchemaMeta {
    type Origin = FromBox;
}

pub struct CassKeyspaceMeta {
    pub name: String,

    // User defined type name to type
    pub user_defined_type_data_type: HashMap<String, Arc<CassDataType>>,
    pub tables: HashMap<String, Arc<CassTableMeta>>,
    pub views: HashMap<String, Arc<CassMaterializedViewMeta>>,
}

// Owned by CassSchemaMeta
impl FFI for CassKeyspaceMeta {
    type Origin = FromRef;
}

pub struct CassTableMeta {
    pub name: String,
    pub columns_metadata: HashMap<String, CassColumnMeta>,
    pub partition_keys: Vec<String>,
    pub clustering_keys: Vec<String>,
    pub views: HashMap<String, Arc<CassMaterializedViewMeta>>,
}

// Either:
// - owned by CassMaterializedViewMeta - won't be given to user
// - Owned by CassKeyspaceMeta (in Arc), referenced (Weak) by CassMaterializedViewMeta
impl FFI for CassTableMeta {
    type Origin = FromRef;
}

pub struct CassMaterializedViewMeta {
    pub name: String,
    pub view_metadata: CassTableMeta,
    pub base_table: Weak<CassTableMeta>,
}

// Shared ownership by CassKeyspaceMeta and CassTableMeta
impl FFI for CassMaterializedViewMeta {
    type Origin = FromRef;
}

pub struct CassColumnMeta {
    pub name: String,
    pub column_type: Arc<CassDataType>,
    pub column_kind: CassColumnType,
}

// Owned by CassTableMeta
impl FFI for CassColumnMeta {
    type Origin = FromRef;
}

pub unsafe fn create_table_metadata(
    keyspace_name: &str,
    table_name: &str,
    table_metadata: &Table,
    user_defined_types: &HashMap<String, Arc<UserDefinedType>>,
) -> CassTableMeta {
    let mut columns_metadata = HashMap::new();
    table_metadata
        .columns
        .iter()
        .for_each(|(column_name, column_metadata)| {
            let cass_column_meta = CassColumnMeta {
                name: column_name.clone(),
                column_type: Arc::new(get_column_type_from_cql_type(
                    &column_metadata.type_,
                    user_defined_types,
                    keyspace_name,
                )),
                column_kind: match column_metadata.kind {
                    ColumnKind::Regular => CassColumnType::CASS_COLUMN_TYPE_REGULAR,
                    ColumnKind::Static => CassColumnType::CASS_COLUMN_TYPE_STATIC,
                    ColumnKind::Clustering => CassColumnType::CASS_COLUMN_TYPE_CLUSTERING_KEY,
                    ColumnKind::PartitionKey => CassColumnType::CASS_COLUMN_TYPE_PARTITION_KEY,
                },
            };

            columns_metadata.insert(column_name.clone(), cass_column_meta);
        });

    CassTableMeta {
        name: table_name.to_owned(),
        columns_metadata,
        partition_keys: table_metadata.partition_key.clone(),
        clustering_keys: table_metadata.clustering_key.clone(),
        views: HashMap::new(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_schema_meta_free(schema_meta: CassOwnedMutPtr<CassSchemaMeta>) {
    BoxFFI::free(schema_meta);
}

#[no_mangle]
pub unsafe extern "C" fn cass_schema_meta_keyspace_by_name(
    schema_meta: CassBorrowedPtr<CassSchemaMeta>,
    keyspace_name: *const c_char,
) -> CassBorrowedPtr<CassKeyspaceMeta> {
    cass_schema_meta_keyspace_by_name_n(schema_meta, keyspace_name, strlen(keyspace_name))
}

#[no_mangle]
pub unsafe extern "C" fn cass_schema_meta_keyspace_by_name_n(
    schema_meta: CassBorrowedPtr<CassSchemaMeta>,
    keyspace_name: *const c_char,
    keyspace_name_length: size_t,
) -> CassBorrowedPtr<CassKeyspaceMeta> {
    if keyspace_name.is_null() {
        return RefFFI::null();
    }

    let metadata = BoxFFI::as_ref(schema_meta).unwrap();
    let keyspace = ptr_to_cstr_n(keyspace_name, keyspace_name_length).unwrap();

    let keyspace_meta = metadata.keyspaces.get(keyspace);

    match keyspace_meta {
        Some(meta) => RefFFI::as_ptr(meta),
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_name(
    keyspace_meta: CassBorrowedPtr<CassKeyspaceMeta>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let keyspace_meta = RefFFI::as_ref(keyspace_meta).unwrap();
    write_str_to_c(keyspace_meta.name.as_str(), name, name_length)
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_user_type_by_name(
    keyspace_meta: CassBorrowedPtr<CassKeyspaceMeta>,
    type_: *const c_char,
) -> CassBorrowedPtr<CassDataType> {
    cass_keyspace_meta_user_type_by_name_n(keyspace_meta, type_, strlen(type_))
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_user_type_by_name_n(
    keyspace_meta: CassBorrowedPtr<CassKeyspaceMeta>,
    type_: *const c_char,
    type_length: size_t,
) -> CassBorrowedPtr<CassDataType> {
    if type_.is_null() {
        return ArcFFI::null();
    }

    let keyspace_meta = RefFFI::as_ref(keyspace_meta).unwrap();
    let user_type_name = ptr_to_cstr_n(type_, type_length).unwrap();

    match keyspace_meta
        .user_defined_type_data_type
        .get(user_type_name)
    {
        Some(udt) => ArcFFI::as_ptr(udt),
        None => ArcFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_table_by_name(
    keyspace_meta: CassBorrowedPtr<CassKeyspaceMeta>,
    table: *const c_char,
) -> CassBorrowedPtr<CassTableMeta> {
    cass_keyspace_meta_table_by_name_n(keyspace_meta, table, strlen(table))
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_table_by_name_n(
    keyspace_meta: CassBorrowedPtr<CassKeyspaceMeta>,
    table: *const c_char,
    table_length: size_t,
) -> CassBorrowedPtr<CassTableMeta> {
    if table.is_null() {
        return RefFFI::null();
    }

    let keyspace_meta = RefFFI::as_ref(keyspace_meta).unwrap();
    let table_name = ptr_to_cstr_n(table, table_length).unwrap();

    let table_meta = keyspace_meta.tables.get(table_name);

    match table_meta {
        Some(meta) => RefFFI::as_ptr(meta),
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_name(
    table_meta: CassBorrowedPtr<CassTableMeta>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let table_meta = RefFFI::as_ref(table_meta).unwrap();
    write_str_to_c(table_meta.name.as_str(), name, name_length)
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_column_count(
    table_meta: CassBorrowedPtr<CassTableMeta>,
) -> size_t {
    let table_meta = RefFFI::as_ref(table_meta).unwrap();
    table_meta.columns_metadata.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_partition_key(
    table_meta: CassBorrowedPtr<CassTableMeta>,
    index: size_t,
) -> CassBorrowedPtr<CassColumnMeta> {
    let table_meta = RefFFI::as_ref(table_meta).unwrap();

    match table_meta.partition_keys.get(index as usize) {
        Some(column_name) => match table_meta.columns_metadata.get(column_name) {
            Some(column_meta) => RefFFI::as_ptr(column_meta),
            None => RefFFI::null(),
        },
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_partition_key_count(
    table_meta: CassBorrowedPtr<CassTableMeta>,
) -> size_t {
    let table_meta = RefFFI::as_ref(table_meta).unwrap();
    table_meta.partition_keys.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_clustering_key(
    table_meta: CassBorrowedPtr<CassTableMeta>,
    index: size_t,
) -> CassBorrowedPtr<CassColumnMeta> {
    let table_meta = RefFFI::as_ref(table_meta).unwrap();

    match table_meta.clustering_keys.get(index as usize) {
        Some(column_name) => match table_meta.columns_metadata.get(column_name) {
            Some(column_meta) => RefFFI::as_ptr(column_meta),
            None => RefFFI::null(),
        },
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_clustering_key_count(
    table_meta: CassBorrowedPtr<CassTableMeta>,
) -> size_t {
    let table_meta = RefFFI::as_ref(table_meta).unwrap();
    table_meta.clustering_keys.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_column_by_name(
    table_meta: CassBorrowedPtr<CassTableMeta>,
    column: *const c_char,
) -> CassBorrowedPtr<CassColumnMeta> {
    cass_table_meta_column_by_name_n(table_meta, column, strlen(column))
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_column_by_name_n(
    table_meta: CassBorrowedPtr<CassTableMeta>,
    column: *const c_char,
    column_length: size_t,
) -> CassBorrowedPtr<CassColumnMeta> {
    if column.is_null() {
        return RefFFI::null();
    }

    let table_meta = RefFFI::as_ref(table_meta).unwrap();
    let column_name = ptr_to_cstr_n(column, column_length).unwrap();

    match table_meta.columns_metadata.get(column_name) {
        Some(column_meta) => RefFFI::as_ptr(column_meta),
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_column_meta_name(
    column_meta: CassBorrowedPtr<CassColumnMeta>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let column_meta = RefFFI::as_ref(column_meta).unwrap();
    write_str_to_c(column_meta.name.as_str(), name, name_length)
}

#[no_mangle]
pub unsafe extern "C" fn cass_column_meta_data_type(
    column_meta: CassBorrowedPtr<CassColumnMeta>,
) -> CassBorrowedPtr<CassDataType> {
    let column_meta = RefFFI::as_ref(column_meta).unwrap();
    ArcFFI::as_ptr(&column_meta.column_type)
}

#[no_mangle]
pub unsafe extern "C" fn cass_column_meta_type(
    column_meta: CassBorrowedPtr<CassColumnMeta>,
) -> CassColumnType {
    let column_meta = RefFFI::as_ref(column_meta).unwrap();
    column_meta.column_kind
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_materialized_view_by_name(
    keyspace_meta: CassBorrowedPtr<CassKeyspaceMeta>,
    view: *const c_char,
) -> CassBorrowedPtr<CassMaterializedViewMeta> {
    cass_keyspace_meta_materialized_view_by_name_n(keyspace_meta, view, strlen(view))
}

#[no_mangle]
pub unsafe extern "C" fn cass_keyspace_meta_materialized_view_by_name_n(
    keyspace_meta: CassBorrowedPtr<CassKeyspaceMeta>,
    view: *const c_char,
    view_length: size_t,
) -> CassBorrowedPtr<CassMaterializedViewMeta> {
    if view.is_null() {
        return RefFFI::null();
    }

    let keyspace_meta = RefFFI::as_ref(keyspace_meta).unwrap();
    let view_name = ptr_to_cstr_n(view, view_length).unwrap();

    match keyspace_meta.views.get(view_name) {
        Some(view_meta) => RefFFI::as_ptr(view_meta.as_ref()),
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_materialized_view_by_name(
    table_meta: CassBorrowedPtr<CassTableMeta>,
    view: *const c_char,
) -> CassBorrowedPtr<CassMaterializedViewMeta> {
    cass_table_meta_materialized_view_by_name_n(table_meta, view, strlen(view))
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_materialized_view_by_name_n(
    table_meta: CassBorrowedPtr<CassTableMeta>,
    view: *const c_char,
    view_length: size_t,
) -> CassBorrowedPtr<CassMaterializedViewMeta> {
    if view.is_null() {
        return RefFFI::null();
    }

    let table_meta = RefFFI::as_ref(table_meta).unwrap();
    let view_name = ptr_to_cstr_n(view, view_length).unwrap();

    match table_meta.views.get(view_name) {
        Some(view_meta) => RefFFI::as_ptr(view_meta.as_ref()),
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_materialized_view_count(
    table_meta: CassBorrowedPtr<CassTableMeta>,
) -> size_t {
    let table_meta = RefFFI::as_ref(table_meta).unwrap();
    table_meta.views.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_table_meta_materialized_view(
    table_meta: CassBorrowedPtr<CassTableMeta>,
    index: size_t,
) -> CassBorrowedPtr<CassMaterializedViewMeta> {
    let table_meta = RefFFI::as_ref(table_meta).unwrap();

    match table_meta.views.iter().nth(index as usize) {
        Some(view_meta) => RefFFI::as_ptr(view_meta.1.as_ref()),
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_materialized_view_meta_column_by_name(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
    column: *const c_char,
) -> CassBorrowedPtr<CassColumnMeta> {
    cass_materialized_view_meta_column_by_name_n(view_meta, column, strlen(column))
}

#[no_mangle]
pub unsafe extern "C" fn cass_materialized_view_meta_column_by_name_n(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
    column: *const c_char,
    column_length: size_t,
) -> CassBorrowedPtr<CassColumnMeta> {
    if column.is_null() {
        return RefFFI::null();
    }

    let view_meta = RefFFI::as_ref(view_meta).unwrap();
    let column_name = ptr_to_cstr_n(column, column_length).unwrap();

    match view_meta.view_metadata.columns_metadata.get(column_name) {
        Some(column_meta) => RefFFI::as_ptr(column_meta),
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_materialized_view_meta_name(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
    name: *mut *const c_char,
    name_length: *mut size_t,
) {
    let view_meta = RefFFI::as_ref(view_meta).unwrap();
    write_str_to_c(view_meta.name.as_str(), name, name_length)
}

#[no_mangle]
pub unsafe extern "C" fn cass_materialized_view_meta_base_table(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
) -> CassBorrowedPtr<CassTableMeta> {
    let view_meta = RefFFI::as_ref(view_meta).unwrap();

    RefFFI::weak_as_ptr(&view_meta.base_table)
}

#[no_mangle]
pub unsafe extern "C" fn cass_materialized_view_meta_column_count(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
) -> size_t {
    let view_meta = RefFFI::as_ref(view_meta).unwrap();
    view_meta.view_metadata.columns_metadata.len() as size_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_materialized_view_meta_column(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
    index: size_t,
) -> CassBorrowedPtr<CassColumnMeta> {
    let view_meta = RefFFI::as_ref(view_meta).unwrap();

    match view_meta
        .view_metadata
        .columns_metadata
        .iter()
        .nth(index as usize)
    {
        Some(column_entry) => RefFFI::as_ptr(column_entry.1),
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_materialized_view_meta_partition_key_count(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
) -> size_t {
    let view_meta = RefFFI::as_ref(view_meta).unwrap();
    view_meta.view_metadata.partition_keys.len() as size_t
}

pub unsafe extern "C" fn cass_materialized_view_meta_partition_key(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
    index: size_t,
) -> CassBorrowedPtr<CassColumnMeta> {
    let view_meta = RefFFI::as_ref(view_meta).unwrap();

    match view_meta.view_metadata.partition_keys.get(index as usize) {
        Some(column_name) => match view_meta.view_metadata.columns_metadata.get(column_name) {
            Some(column_meta) => RefFFI::as_ptr(column_meta),
            None => RefFFI::null(),
        },
        None => RefFFI::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_materialized_view_meta_clustering_key_count(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
) -> size_t {
    let view_meta = RefFFI::as_ref(view_meta).unwrap();
    view_meta.view_metadata.clustering_keys.len() as size_t
}

pub unsafe extern "C" fn cass_materialized_view_meta_clustering_key(
    view_meta: CassBorrowedPtr<CassMaterializedViewMeta>,
    index: size_t,
) -> CassBorrowedPtr<CassColumnMeta> {
    let view_meta = RefFFI::as_ref(view_meta).unwrap();

    match view_meta.view_metadata.clustering_keys.get(index as usize) {
        Some(column_name) => match view_meta.view_metadata.columns_metadata.get(column_name) {
            Some(column_meta) => RefFFI::as_ptr(column_meta),
            None => RefFFI::null(),
        },
        None => RefFFI::null(),
    }
}
