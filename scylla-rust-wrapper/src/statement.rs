use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::exec_profile::PerStatementExecProfile;
use crate::prepared::CassPrepared;
use crate::query_result::CassResult;
use crate::retry_policy::CassRetryPolicy;
use crate::types::*;
use crate::value::CassCqlValue;
use crate::{argconv::*, value};
use scylla::frame::types::Consistency;
use scylla::frame::value::MaybeUnset;
use scylla::frame::value::MaybeUnset::{Set, Unset};
use scylla::query::Query;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::RowWriter;
use scylla::serialize::SerializationError;
use scylla::statement::SerialConsistency;
use scylla::transport::{PagingState, PagingStateResponse};
use std::collections::HashMap;
use std::convert::TryInto;
use std::os::raw::{c_char, c_int};
use std::slice;
use std::sync::Arc;

use thiserror::Error;

#[derive(Clone)]
pub enum BoundStatement {
    Simple(BoundSimpleQuery),
    Prepared(BoundPreparedStatement),
}

#[derive(Clone)]
pub struct BoundPreparedStatement {
    // Arc is needed, because PreparedStatement is passed by reference to session.execute
    pub statement: Arc<CassPrepared>,
    pub bound_values: Vec<MaybeUnset<Option<CassCqlValue>>>,
}

impl BoundPreparedStatement {
    fn bind_cql_value(&mut self, index: usize, value: Option<CassCqlValue>) -> CassError {
        match (
            self.bound_values.get_mut(index),
            self.statement.variable_col_data_types.get(index),
        ) {
            (Some(v), Some(dt)) => {
                // Perform the typecheck.
                if !value::is_type_compatible(&value, dt) {
                    return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
                }

                *v = Set(value);
                CassError::CASS_OK
            }
            (None, None) => CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
            // This indicates a length mismatch between col specs table and self.bound_values.
            //
            // It can only occur when user provides bad `count` value in `cass_statement_reset_parameters`.
            // Cpp-driver does not verify that both of these values are equal.
            // I believe returning CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS is best we can do here.
            _ => CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
        }
    }

    fn bind_cql_value_by_name(
        &mut self,
        name: &str,
        is_case_sensitive: bool,
        value: Option<CassCqlValue>,
    ) -> CassError {
        let indices: Vec<usize> = self
            .statement
            .statement
            .get_variable_col_specs()
            .iter()
            .enumerate()
            .filter(|(_, col)| {
                is_case_sensitive && col.name() == name
                    || !is_case_sensitive && col.name().eq_ignore_ascii_case(name)
            })
            .map(|(i, _)| i)
            .collect();

        if indices.is_empty() {
            return CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST;
        }

        self.bind_multiple_values_by_name(&indices, value)
    }

    fn bind_multiple_values_by_name(
        &mut self,
        indices: &[usize],
        value: Option<CassCqlValue>,
    ) -> CassError {
        for i in indices {
            let bind_status = self.bind_cql_value(*i, value.clone());

            if bind_status != CassError::CASS_OK {
                return bind_status;
            }
        }

        CassError::CASS_OK
    }
}

#[derive(Clone)]
pub struct BoundSimpleQuery {
    pub query: Query,
    pub bound_values: Vec<MaybeUnset<Option<CassCqlValue>>>,
    pub name_to_bound_index: HashMap<String, usize>,
}

impl BoundSimpleQuery {
    fn bind_cql_value(&mut self, index: usize, value: Option<CassCqlValue>) -> CassError {
        match self.bound_values.get_mut(index) {
            Some(v) => {
                *v = Set(value);
                CassError::CASS_OK
            }
            None => CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
        }
    }

    fn bind_cql_value_by_name(&mut self, name: &str, value: Option<CassCqlValue>) -> CassError {
        let index = self.name_to_bound_index.get(name);

        if let Some(idx) = index {
            self.bind_cql_value(*idx, value)
        } else {
            let index = {
                // If new name appeared, we want to append its value to the vector.
                // This can possibly overwrite some already Set value
                // (which was set via by_index binding). cpp-driver does the same.
                let free_index = self.name_to_bound_index.len();

                // New index exceeds the number of declared parameters.
                // cpp-driver returns this error as well.
                if free_index >= self.bound_values.len() {
                    return CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST;
                }
                free_index
            };

            self.name_to_bound_index.insert(name.to_string(), index);
            self.bind_cql_value(index, value)
        }
    }
}

/// Used to provide a custom serialization implementation for unprepared queries.
///
/// Users are allowed to bind values by either position, or name.
/// This is done via `cass_statement_bind_*` API.
///
/// In case of binding by position, it's really simple - we simply
/// append the value at the end of `bound_values` struct.
///
/// When binding by name, we append the value at the end of `bound_values`,
/// but we also store the name-to-index mapping in `name_to_bound_index` map.
/// Having this information, and prepared metadata provided in serialization context,
/// we can build a resulting vector of bound values.
pub struct SimpleQueryRowSerializer {
    pub bound_values: Vec<MaybeUnset<Option<CassCqlValue>>>,
    pub name_to_bound_index: HashMap<String, usize>,
}

#[derive(Debug, Error)]
#[error("Unknown named parameter \"{0}\"")]
pub struct UnknownNamedParameterError(String);

impl SerializeRow for SimpleQueryRowSerializer {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        if self.name_to_bound_index.is_empty() {
            // Values bound by position - no need to reorder anything.
            <_ as SerializeRow>::serialize(&self.bound_values, ctx, writer)
        } else {
            // Values bound by names - we need to make use of col specs from metadata to
            // bind the values in correct order.
            ctx.columns().iter().try_for_each(|col_spec| {
                let bound_values_idx =
                    self.name_to_bound_index
                        .get(col_spec.name())
                        .ok_or_else(|| {
                            SerializationError::new(UnknownNamedParameterError(
                                col_spec.name().to_owned(),
                            ))
                        })?;

                let val = &self.bound_values[*bound_values_idx];

                <_ as SerializeValue>::serialize(val, col_spec.typ(), writer.make_cell_writer())?;

                Ok(())
            })
        }
    }

    fn is_empty(&self) -> bool {
        self.bound_values.is_empty()
    }
}

pub struct CassStatement {
    pub statement: BoundStatement,
    pub paging_state: PagingState,
    pub paging_enabled: bool,
    pub request_timeout_ms: Option<cass_uint64_t>,

    pub(crate) exec_profile: Option<PerStatementExecProfile>,
}

impl BoxFFI for CassStatement {}

impl CassStatement {
    fn bind_cql_value(&mut self, index: usize, value: Option<CassCqlValue>) -> CassError {
        match &mut self.statement {
            BoundStatement::Simple(simple) => simple.bind_cql_value(index, value),
            BoundStatement::Prepared(prepared) => prepared.bind_cql_value(index, value),
        }
    }

    fn bind_cql_value_by_name(&mut self, name: &str, value: Option<CassCqlValue>) -> CassError {
        // If the name was quoted, then we should treat it as case sensitive.
        let (name_unquoted, is_case_sensitive) =
            match name.strip_prefix('\"').and_then(|s| s.strip_suffix('\"')) {
                Some(name_unquoted) => (name_unquoted, true),
                None => (name, false),
            };

        match &mut self.statement {
            BoundStatement::Simple(simple) => simple.bind_cql_value_by_name(name_unquoted, value),
            BoundStatement::Prepared(prepared) => {
                prepared.bind_cql_value_by_name(name_unquoted, is_case_sensitive, value)
            }
        }
    }

    fn reset_bound_values(&mut self, count: usize) {
        // Clear bound values and resize the vector - all values should be unset.
        match &mut self.statement {
            BoundStatement::Simple(simple) => {
                simple.bound_values.clear();
                simple.bound_values.resize(count, Unset);
                simple.name_to_bound_index.clear();
            }
            BoundStatement::Prepared(prepared) => {
                prepared.bound_values.clear();
                prepared.bound_values.resize(count, Unset);
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_new(
    query: *const c_char,
    parameter_count: size_t,
) -> CassExclusiveMutPtr<CassStatement> {
    cass_statement_new_n(query, strlen(query), parameter_count)
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_new_n(
    query: *const c_char,
    query_length: size_t,
    parameter_count: size_t,
) -> CassExclusiveMutPtr<CassStatement> {
    let query_str = match ptr_to_cstr_n(query, query_length) {
        Some(v) => v,
        None => return BoxFFI::null_mut(),
    };

    let query = Query::new(query_str.to_string());

    let simple_query = BoundSimpleQuery {
        query,
        bound_values: vec![Unset; parameter_count as usize],
        name_to_bound_index: HashMap::with_capacity(parameter_count as usize),
    };

    BoxFFI::into_ptr(Box::new(CassStatement {
        statement: BoundStatement::Simple(simple_query),
        paging_state: PagingState::start(),
        // Cpp driver disables paging by default.
        paging_enabled: false,
        request_timeout_ms: None,
        exec_profile: None,
    }))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_free(statement_raw: CassExclusiveMutPtr<CassStatement>) {
    BoxFFI::free(statement_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_consistency(
    mut statement: CassExclusiveMutPtr<CassStatement>,
    consistency: CassConsistency,
) -> CassError {
    let consistency_opt = get_consistency_from_cass_consistency(consistency);

    if let Some(consistency) = consistency_opt {
        match &mut BoxFFI::as_mut_ref(&mut statement).unwrap().statement {
            BoundStatement::Simple(inner) => inner.query.set_consistency(consistency),
            BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
                .statement
                .set_consistency(consistency),
        }
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_paging_size(
    mut statement_raw: CassExclusiveMutPtr<CassStatement>,
    page_size: c_int,
) -> CassError {
    let statement = BoxFFI::as_mut_ref(&mut statement_raw).unwrap();
    if page_size <= 0 {
        // Cpp driver sets the page size flag only for positive page size provided by user.
        statement.paging_enabled = false;
    } else {
        statement.paging_enabled = true;
        match &mut statement.statement {
            BoundStatement::Simple(inner) => inner.query.set_page_size(page_size),
            BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
                .statement
                .set_page_size(page_size),
        }
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_paging_state(
    mut statement: CassExclusiveMutPtr<CassStatement>,
    result: CassSharedPtr<CassResult>,
) -> CassError {
    let statement = BoxFFI::as_mut_ref(&mut statement).unwrap();
    let result = ArcFFI::as_ref(&result).unwrap();

    match &result.paging_state_response {
        PagingStateResponse::HasMorePages { state } => statement.paging_state.clone_from(state),
        PagingStateResponse::NoMorePages => statement.paging_state = PagingState::start(),
    }
    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_paging_state_token(
    mut statement: CassExclusiveMutPtr<CassStatement>,
    paging_state: *const c_char,
    paging_state_size: size_t,
) -> CassError {
    let statement_from_raw = BoxFFI::as_mut_ref(&mut statement).unwrap();

    if paging_state.is_null() {
        statement_from_raw.paging_state = PagingState::start();
        return CassError::CASS_ERROR_LIB_NULL_VALUE;
    }

    let paging_state_usize: usize = paging_state_size.try_into().unwrap();
    let paging_state_bytes = slice::from_raw_parts(paging_state as *const u8, paging_state_usize);
    statement_from_raw.paging_state = PagingState::new_from_raw_bytes(paging_state_bytes);
    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_is_idempotent(
    mut statement_raw: CassExclusiveMutPtr<CassStatement>,
    is_idempotent: cass_bool_t,
) -> CassError {
    match &mut BoxFFI::as_mut_ref(&mut statement_raw).unwrap().statement {
        BoundStatement::Simple(inner) => inner.query.set_is_idempotent(is_idempotent != 0),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_is_idempotent(is_idempotent != 0),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_tracing(
    mut statement_raw: CassExclusiveMutPtr<CassStatement>,
    enabled: cass_bool_t,
) -> CassError {
    match &mut BoxFFI::as_mut_ref(&mut statement_raw).unwrap().statement {
        BoundStatement::Simple(inner) => inner.query.set_tracing(enabled != 0),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_tracing(enabled != 0),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_retry_policy(
    mut statement: CassExclusiveMutPtr<CassStatement>,
    retry_policy: CassSharedPtr<CassRetryPolicy>,
) -> CassError {
    let maybe_arced_retry_policy: Option<Arc<dyn scylla::retry_policy::RetryPolicy>> =
        ArcFFI::as_ref(&retry_policy).map(|policy| match policy {
            CassRetryPolicy::DefaultRetryPolicy(default) => {
                default.clone() as Arc<dyn scylla::retry_policy::RetryPolicy>
            }
            CassRetryPolicy::FallthroughRetryPolicy(fallthrough) => fallthrough.clone(),
            CassRetryPolicy::DowngradingConsistencyRetryPolicy(downgrading) => downgrading.clone(),
        });

    match &mut BoxFFI::as_mut_ref(&mut statement).unwrap().statement {
        BoundStatement::Simple(inner) => inner.query.set_retry_policy(maybe_arced_retry_policy),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_retry_policy(maybe_arced_retry_policy),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_serial_consistency(
    mut statement: CassExclusiveMutPtr<CassStatement>,
    serial_consistency: CassConsistency,
) -> CassError {
    // cpp-driver doesn't validate passed value in any way.
    // If it is an incorrect serial-consistency value then it will be set
    // and sent as-is.
    // Before adapting the driver to Rust Driver 0.12 this code
    // set serial consistency if a user passed correct value and set it to
    // None otherwise.
    // I think that failing explicitly is a better idea, so I decided to return
    // and error
    let consistency = match get_consistency_from_cass_consistency(serial_consistency) {
        Some(Consistency::Serial) => SerialConsistency::Serial,
        Some(Consistency::LocalSerial) => SerialConsistency::LocalSerial,
        _ => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };

    match &mut BoxFFI::as_mut_ref(&mut statement).unwrap().statement {
        BoundStatement::Simple(inner) => inner.query.set_serial_consistency(Some(consistency)),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_serial_consistency(Some(consistency)),
    }

    CassError::CASS_OK
}

fn get_consistency_from_cass_consistency(consistency: CassConsistency) -> Option<Consistency> {
    match consistency {
        CassConsistency::CASS_CONSISTENCY_ANY => Some(Consistency::Any),
        CassConsistency::CASS_CONSISTENCY_ONE => Some(Consistency::One),
        CassConsistency::CASS_CONSISTENCY_TWO => Some(Consistency::Two),
        CassConsistency::CASS_CONSISTENCY_THREE => Some(Consistency::Three),
        CassConsistency::CASS_CONSISTENCY_QUORUM => Some(Consistency::Quorum),
        CassConsistency::CASS_CONSISTENCY_ALL => Some(Consistency::All),
        CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM => Some(Consistency::LocalQuorum),
        CassConsistency::CASS_CONSISTENCY_EACH_QUORUM => Some(Consistency::EachQuorum),
        CassConsistency::CASS_CONSISTENCY_SERIAL => Some(Consistency::Serial),
        CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL => Some(Consistency::LocalSerial),
        CassConsistency::CASS_CONSISTENCY_LOCAL_ONE => Some(Consistency::LocalOne),
        _ => None,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_timestamp(
    mut statement: CassExclusiveMutPtr<CassStatement>,
    timestamp: cass_int64_t,
) -> CassError {
    match &mut BoxFFI::as_mut_ref(&mut statement).unwrap().statement {
        BoundStatement::Simple(inner) => inner.query.set_timestamp(Some(timestamp)),
        BoundStatement::Prepared(inner) => Arc::make_mut(&mut inner.statement)
            .statement
            .set_timestamp(Some(timestamp)),
    }

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_request_timeout(
    mut statement: CassExclusiveMutPtr<CassStatement>,
    timeout_ms: cass_uint64_t,
) -> CassError {
    // The maximum duration for a sleep is 68719476734 milliseconds (approximately 2.2 years).
    // Note: this is limited by tokio::time:timout
    // https://github.com/tokio-rs/tokio/blob/4b1c4801b1383800932141d0f6508d5b3003323e/tokio/src/time/driver/wheel/mod.rs#L44-L50
    let request_timeout_limit = 2_u64.pow(36) - 1;
    if timeout_ms >= request_timeout_limit {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let statement_from_raw = BoxFFI::as_mut_ref(&mut statement).unwrap();
    statement_from_raw.request_timeout_ms = Some(timeout_ms);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_reset_parameters(
    mut statement_raw: CassExclusiveMutPtr<CassStatement>,
    count: size_t,
) -> CassError {
    let statement = BoxFFI::as_mut_ref(&mut statement_raw).unwrap();
    statement.reset_bound_values(count as usize);

    CassError::CASS_OK
}

prepare_binders_macro!(@index_and_name CassStatement,
    |s: &mut CassStatement, idx, v| s.bind_cql_value(idx, v),
    |s: &mut CassStatement, name, v| s.bind_cql_value_by_name(name, v));
make_binders!(
    null,
    cass_statement_bind_null,
    cass_statement_bind_null_by_name,
    cass_statement_bind_null_by_name_n
);
make_binders!(
    int8,
    cass_statement_bind_int8,
    cass_statement_bind_int8_by_name,
    cass_statement_bind_int8_by_name_n
);
make_binders!(
    int16,
    cass_statement_bind_int16,
    cass_statement_bind_int16_by_name,
    cass_statement_bind_int16_by_name_n
);
make_binders!(
    int32,
    cass_statement_bind_int32,
    cass_statement_bind_int32_by_name,
    cass_statement_bind_int32_by_name_n
);
make_binders!(
    uint32,
    cass_statement_bind_uint32,
    cass_statement_bind_uint32_by_name,
    cass_statement_bind_uint32_by_name_n
);
make_binders!(
    int64,
    cass_statement_bind_int64,
    cass_statement_bind_int64_by_name,
    cass_statement_bind_int64_by_name_n
);
make_binders!(
    float,
    cass_statement_bind_float,
    cass_statement_bind_float_by_name,
    cass_statement_bind_float_by_name_n
);
make_binders!(
    double,
    cass_statement_bind_double,
    cass_statement_bind_double_by_name,
    cass_statement_bind_double_by_name_n
);
make_binders!(
    bool,
    cass_statement_bind_bool,
    cass_statement_bind_bool_by_name,
    cass_statement_bind_bool_by_name_n
);
make_binders!(
    string,
    cass_statement_bind_string,
    string,
    cass_statement_bind_string_by_name,
    string_n,
    cass_statement_bind_string_by_name_n
);
make_binders!(@index string_n, cass_statement_bind_string_n);
make_binders!(
    bytes,
    cass_statement_bind_bytes,
    cass_statement_bind_bytes_by_name,
    cass_statement_bind_bytes_by_name_n
);
make_binders!(
    uuid,
    cass_statement_bind_uuid,
    cass_statement_bind_uuid_by_name,
    cass_statement_bind_uuid_by_name_n
);
make_binders!(
    inet,
    cass_statement_bind_inet,
    cass_statement_bind_inet_by_name,
    cass_statement_bind_inet_by_name_n
);
make_binders!(
    duration,
    cass_statement_bind_duration,
    cass_statement_bind_duration_by_name,
    cass_statement_bind_duration_by_name_n
);
make_binders!(
    decimal,
    cass_statement_bind_decimal,
    cass_statement_bind_decimal_by_name,
    cass_statement_bind_decimal_by_name_n
);
make_binders!(
    collection,
    cass_statement_bind_collection,
    cass_statement_bind_collection_by_name,
    cass_statement_bind_collection_by_name_n
);
make_binders!(
    tuple,
    cass_statement_bind_tuple,
    cass_statement_bind_tuple_by_name,
    cass_statement_bind_tuple_by_name_n
);
make_binders!(
    user_type,
    cass_statement_bind_user_type,
    cass_statement_bind_user_type_by_name,
    cass_statement_bind_user_type_by_name_n
);
