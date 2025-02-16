use crate::argconv::{
    ArcFFI, BoxFFI, CassExclusiveConstPtr, CassExclusiveMutPtr, CassSharedPtr, OwnershipExclusive,
    FFI,
};
use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::cass_types::{make_batch_type, CassBatchType};
use crate::exec_profile::PerStatementExecProfile;
use crate::retry_policy::CassRetryPolicy;
use crate::statement::{BoundStatement, CassStatement};
use crate::types::*;
use crate::value::CassCqlValue;
use scylla::batch::Batch;
use scylla::frame::value::MaybeUnset;
use std::convert::TryInto;
use std::sync::Arc;

pub struct CassBatch {
    pub state: Arc<CassBatchState>,
    pub batch_request_timeout_ms: Option<cass_uint64_t>,

    pub(crate) exec_profile: Option<PerStatementExecProfile>,
}

impl FFI for CassBatch {
    type Ownership = OwnershipExclusive;
}

#[derive(Clone)]
pub struct CassBatchState {
    pub batch: Batch,
    pub bound_values: Vec<Vec<MaybeUnset<Option<CassCqlValue>>>>,
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_new(type_: CassBatchType) -> CassExclusiveMutPtr<CassBatch> {
    if let Some(batch_type) = make_batch_type(type_) {
        BoxFFI::into_ptr(Box::new(CassBatch {
            state: Arc::new(CassBatchState {
                batch: Batch::new(batch_type),
                bound_values: Vec::new(),
            }),
            batch_request_timeout_ms: None,
            exec_profile: None,
        }))
    } else {
        BoxFFI::null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_free(batch: CassExclusiveMutPtr<CassBatch>) {
    BoxFFI::free(batch);
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_consistency(
    mut batch: CassExclusiveMutPtr<CassBatch>,
    consistency: CassConsistency,
) -> CassError {
    let batch = BoxFFI::as_mut_ref(&mut batch).unwrap();
    let consistency = match consistency.try_into().ok() {
        Some(c) => c,
        None => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };
    Arc::make_mut(&mut batch.state)
        .batch
        .set_consistency(consistency);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_serial_consistency(
    mut batch: CassExclusiveMutPtr<CassBatch>,
    serial_consistency: CassConsistency,
) -> CassError {
    let batch = BoxFFI::as_mut_ref(&mut batch).unwrap();
    let serial_consistency = match serial_consistency.try_into().ok() {
        Some(c) => c,
        None => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };
    Arc::make_mut(&mut batch.state)
        .batch
        .set_serial_consistency(Some(serial_consistency));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_retry_policy(
    mut batch: CassExclusiveMutPtr<CassBatch>,
    retry_policy: CassSharedPtr<CassRetryPolicy>,
) -> CassError {
    let batch = BoxFFI::as_mut_ref(&mut batch).unwrap();

    let maybe_arced_retry_policy: Option<Arc<dyn scylla::retry_policy::RetryPolicy>> =
        ArcFFI::as_ref(&retry_policy).map(|policy| match policy {
            CassRetryPolicy::DefaultRetryPolicy(default) => {
                default.clone() as Arc<dyn scylla::retry_policy::RetryPolicy>
            }
            CassRetryPolicy::FallthroughRetryPolicy(fallthrough) => fallthrough.clone(),
            CassRetryPolicy::DowngradingConsistencyRetryPolicy(downgrading) => downgrading.clone(),
        });

    Arc::make_mut(&mut batch.state)
        .batch
        .set_retry_policy(maybe_arced_retry_policy);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_timestamp(
    mut batch: CassExclusiveMutPtr<CassBatch>,
    timestamp: cass_int64_t,
) -> CassError {
    let batch = BoxFFI::as_mut_ref(&mut batch).unwrap();

    Arc::make_mut(&mut batch.state)
        .batch
        .set_timestamp(Some(timestamp));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_request_timeout(
    mut batch: CassExclusiveMutPtr<CassBatch>,
    timeout_ms: cass_uint64_t,
) -> CassError {
    let batch = BoxFFI::as_mut_ref(&mut batch).unwrap();
    batch.batch_request_timeout_ms = Some(timeout_ms);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_is_idempotent(
    mut batch: CassExclusiveMutPtr<CassBatch>,
    is_idempotent: cass_bool_t,
) -> CassError {
    let batch = BoxFFI::as_mut_ref(&mut batch).unwrap();
    Arc::make_mut(&mut batch.state)
        .batch
        .set_is_idempotent(is_idempotent != 0);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_tracing(
    mut batch: CassExclusiveMutPtr<CassBatch>,
    enabled: cass_bool_t,
) -> CassError {
    let batch = BoxFFI::as_mut_ref(&mut batch).unwrap();
    Arc::make_mut(&mut batch.state)
        .batch
        .set_tracing(enabled != 0);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_add_statement(
    mut batch: CassExclusiveMutPtr<CassBatch>,
    statement: CassExclusiveConstPtr<CassStatement>,
) -> CassError {
    let batch = BoxFFI::as_mut_ref(&mut batch).unwrap();
    let state = Arc::make_mut(&mut batch.state);
    let statement = BoxFFI::as_ref(&statement).unwrap();

    match &statement.statement {
        BoundStatement::Simple(q) => {
            state.batch.append_statement(q.query.clone());
            state.bound_values.push(q.bound_values.clone());
        }
        BoundStatement::Prepared(p) => {
            state.batch.append_statement(p.statement.statement.clone());
            state.bound_values.push(p.bound_values.clone());
        }
    };

    CassError::CASS_OK
}
