use std::convert::{TryFrom, TryInto};
use std::ffi::c_char;
use std::future::Future;
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use scylla::execution_profile::{
    ExecutionProfile, ExecutionProfileBuilder, ExecutionProfileHandle,
};
use scylla::load_balancing::LatencyAwarenessBuilder;
use scylla::retry_policy::RetryPolicy;
use scylla::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::statement::Consistency;

use crate::argconv::{
    ptr_to_cstr_n, strlen, ArcFFI, BoxFFI, CassBorrowedMutPtr, CassBorrowedPtr, CassOwnedMutPtr,
    FromBox, FFI,
};
use crate::batch::CassBatch;
use crate::cass_error::CassError;
use crate::cass_types::CassConsistency;
use crate::cluster::{
    set_load_balance_dc_aware_n, set_load_balance_rack_aware_n, LoadBalancingConfig,
    LoadBalancingKind,
};
use crate::retry_policy::CassRetryPolicy;
use crate::retry_policy::RetryPolicy::{
    DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy,
};
use crate::session::CassSessionInner;
use crate::statement::CassStatement;
use crate::types::{
    cass_bool_t, cass_double_t, cass_int32_t, cass_int64_t, cass_uint32_t, cass_uint64_t, size_t,
};

#[derive(Clone, Debug)]
pub struct CassExecProfile {
    inner: ExecutionProfileBuilder,
    load_balancing_config: LoadBalancingConfig,
}

impl FFI for CassExecProfile {
    type Origin = FromBox;
}

impl CassExecProfile {
    fn new() -> Self {
        Self {
            inner: ExecutionProfile::builder(),
            load_balancing_config: Default::default(),
        }
    }

    pub(crate) async fn build(
        self,
        cluster_default_profile: &ExecutionProfile,
    ) -> ExecutionProfile {
        let load_balacing = if self.load_balancing_config.load_balancing_kind.is_some() {
            self.load_balancing_config.build().await
        } else {
            // If load balancing config does not have LB kind defined,
            // we make use of cluster's LBP.
            cluster_default_profile.get_load_balancing_policy().clone()
        };

        self.inner.load_balancing_policy(load_balacing).build()
    }
}

/// Represents a non-empty execution profile name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecProfileName(String);

#[derive(Debug, PartialEq, Eq)]
pub struct EmptyProfileName;

impl TryFrom<String> for ExecProfileName {
    type Error = EmptyProfileName;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        if name.is_empty() {
            Err(EmptyProfileName)
        } else {
            Ok(ExecProfileName(name))
        }
    }
}

impl Deref for ExecProfileName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// The resolved or not yet resolved execution profile that is specific
// for a particular statement or batch.
#[derive(Debug, Clone)]
pub(crate) struct PerStatementExecProfile(Arc<RwLock<PerStatementExecProfileInner>>);

impl PerStatementExecProfile {
    pub(crate) fn new_unresolved(name: ExecProfileName) -> Self {
        Self(Arc::new(RwLock::new(
            PerStatementExecProfileInner::Unresolved(name),
        )))
    }

    // Clippy claims it is possible to make this `async fn`, but it's terribly wrong,
    // because async fn can't have its future bound to a specific lifetime, which is
    // required in this case.
    #[allow(clippy::manual_async_fn)]
    pub(crate) fn get_or_resolve_profile_handle<'a>(
        &'a self,
        cass_session_inner: &'a CassSessionInner,
    ) -> impl Future<Output = Result<ExecutionProfileHandle, (CassError, String)>> + 'a {
        async move {
            let already_resolved = {
                let read_guard = self.0.read().unwrap();
                match read_guard.deref() {
                    PerStatementExecProfileInner::Unresolved(_) => None,
                    PerStatementExecProfileInner::Resolved(handle) => Some(handle.clone()),
                }
            };

            let handle = if let Some(handle) = already_resolved {
                handle
            } else {
                let inner = &mut *self.0.write().unwrap();
                match &*inner {
                    PerStatementExecProfileInner::Unresolved(name) => {
                        let handle = cass_session_inner.resolve_exec_profile(name)?;
                        *inner = PerStatementExecProfileInner::Resolved(handle.clone());
                        handle
                    }
                    PerStatementExecProfileInner::Resolved(handle) => handle,
                }
                .clone()
            };

            Ok(handle)
        }
    }
}

// The resolved or not yet resolved execution profile that is specific
// for a particular statement or batch.
#[derive(Debug)]
pub(crate) enum PerStatementExecProfileInner {
    // This is set eagerly on `cass_{statement,batch}_set_execution_profile()` call. CPP driver resolves
    // the execution profile's name in the global hashmap on each query. As we want to avoid it,
    // we resolve it only once, lazily, on the first query after the name it set. The reason why
    // it can't be set eagerly is simple: the mentioned function does not provide any access
    // to the Session.
    Unresolved(ExecProfileName),

    // This is the handle to be cloned as per-statement handle upon each query.
    // We have to do that due to limitations of CPP driver API:
    // `cass_{statement,batch}_set_execution_profile()` does not have access to exec profiles map,
    // whereas `cass_session_execute[_batch]()` can't mutate the shared statement
    // (but only its own copy of the statement).
    //
    // The purpose of this is to make it possible to resolve the profile only once after its alterations
    // (e.g. re-setting an exec profile name) take place, and then (on further executions) used straight
    // from here without resolution.
    // In order to achieve that, we want the resolution's result to be saved into the Statement
    // struct that is shared between calls to cass_session_execute().
    // Note that we can't use references to that Statement inside of a CassFuture returned from
    // `cass_session_execute()` because of lifetime issues (possible use-after-free).
    // Therefore, we have to clone Statement's contents to inside the future and perform resolution
    // there. If this struct weren't shared under Arc and we cloned it into the future,
    // then the resolution inside the future would never propagate into the shared Statement struct.
    // The same is true for Arc'ed `PerStatementExecProfileInner` in Batch.
    Resolved(ExecutionProfileHandle),
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_new() -> CassOwnedMutPtr<CassExecProfile> {
    BoxFFI::into_ptr(Box::new(CassExecProfile::new()))
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_free(profile: CassOwnedMutPtr<CassExecProfile>) {
    BoxFFI::free(profile);
}

/* Exec profiles scope setters */

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_execution_profile(
    statement: CassBorrowedMutPtr<CassStatement>,
    name: *const c_char,
) -> CassError {
    cass_statement_set_execution_profile_n(statement, name, strlen(name))
}

#[no_mangle]
pub unsafe extern "C" fn cass_statement_set_execution_profile_n(
    statement: CassBorrowedMutPtr<CassStatement>,
    name: *const c_char,
    name_length: size_t,
) -> CassError {
    let statement = BoxFFI::as_mut_ref(statement).unwrap();
    let name: Option<ExecProfileName> =
        ptr_to_cstr_n(name, name_length).and_then(|name| name.to_owned().try_into().ok());
    statement.exec_profile = name.map(PerStatementExecProfile::new_unresolved);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_execution_profile(
    batch: CassBorrowedMutPtr<CassBatch>,
    name: *const c_char,
) -> CassError {
    cass_batch_set_execution_profile_n(batch, name, strlen(name))
}

#[no_mangle]
pub unsafe extern "C" fn cass_batch_set_execution_profile_n(
    batch: CassBorrowedMutPtr<CassBatch>,
    name: *const c_char,
    name_length: size_t,
) -> CassError {
    let batch = BoxFFI::as_mut_ref(batch).unwrap();
    let name: Option<ExecProfileName> =
        ptr_to_cstr_n(name, name_length).and_then(|name| name.to_owned().try_into().ok());
    batch.exec_profile = name.map(PerStatementExecProfile::new_unresolved);

    CassError::CASS_OK
}

/* Config options setters */

pub(crate) fn exec_profile_builder_modify(
    builder: &mut ExecutionProfileBuilder,
    builder_modifier: impl FnOnce(ExecutionProfileBuilder) -> ExecutionProfileBuilder,
) {
    let taken_builder = std::mem::take(builder);
    let new_builder = builder_modifier(taken_builder);
    *builder = new_builder;
}

impl CassExecProfile {
    fn modify_in_place(
        &mut self,
        builder_modifier: impl FnOnce(ExecutionProfileBuilder) -> ExecutionProfileBuilder,
    ) {
        exec_profile_builder_modify(&mut self.inner, builder_modifier)
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_consistency(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    consistency: CassConsistency,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();
    let consistency: Consistency = match consistency.try_into() {
        Ok(c) => c,
        Err(_) => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
    };

    profile_builder.modify_in_place(|builder| builder.consistency(consistency));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_no_speculative_execution_policy(
    profile: CassBorrowedMutPtr<CassExecProfile>,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();

    profile_builder.modify_in_place(|builder| builder.speculative_execution_policy(None));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_constant_speculative_execution_policy(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    constant_delay_ms: cass_int64_t,
    max_speculative_executions: cass_int32_t,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();
    if constant_delay_ms < 0 || max_speculative_executions < 0 {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let policy = SimpleSpeculativeExecutionPolicy {
        max_retry_count: max_speculative_executions as usize,
        retry_interval: Duration::from_millis(constant_delay_ms as u64),
    };

    profile_builder
        .modify_in_place(|builder| builder.speculative_execution_policy(Some(Arc::new(policy))));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_latency_aware_routing(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    enabled: cass_bool_t,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();
    profile_builder
        .load_balancing_config
        .latency_awareness_enabled = enabled != 0;

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_latency_aware_routing_settings(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    exclusion_threshold: cass_double_t,
    _scale_ms: cass_uint64_t, // Currently ignored, TODO: add this parameter to Rust driver
    retry_period_ms: cass_uint64_t,
    update_rate_ms: cass_uint64_t,
    min_measured: cass_uint64_t,
) {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();
    profile_builder
        .load_balancing_config
        .latency_awareness_builder = LatencyAwarenessBuilder::new()
        .exclusion_threshold(exclusion_threshold)
        .retry_period(Duration::from_millis(retry_period_ms))
        .update_rate(Duration::from_millis(update_rate_ms))
        .minimum_measurements(min_measured as usize);
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_dc_aware(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    local_dc: *const c_char,
    used_hosts_per_remote_dc: cass_uint32_t,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    cass_execution_profile_set_load_balance_dc_aware_n(
        profile,
        local_dc,
        strlen(local_dc),
        used_hosts_per_remote_dc,
        allow_remote_dcs_for_local_cl,
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_dc_aware_n(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    local_dc: *const c_char,
    local_dc_length: size_t,
    used_hosts_per_remote_dc: cass_uint32_t,
    allow_remote_dcs_for_local_cl: cass_bool_t,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();

    set_load_balance_dc_aware_n(
        &mut profile_builder.load_balancing_config,
        local_dc,
        local_dc_length,
        used_hosts_per_remote_dc,
        allow_remote_dcs_for_local_cl,
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_rack_aware(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    local_dc_raw: *const c_char,
    local_rack_raw: *const c_char,
) -> CassError {
    cass_execution_profile_set_load_balance_rack_aware_n(
        profile,
        local_dc_raw,
        strlen(local_dc_raw),
        local_rack_raw,
        strlen(local_rack_raw),
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_rack_aware_n(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    local_dc_raw: *const c_char,
    local_dc_length: size_t,
    local_rack_raw: *const c_char,
    local_rack_length: size_t,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();

    set_load_balance_rack_aware_n(
        &mut profile_builder.load_balancing_config,
        local_dc_raw,
        local_dc_length,
        local_rack_raw,
        local_rack_length,
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_load_balance_round_robin(
    profile: CassBorrowedMutPtr<CassExecProfile>,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();
    profile_builder.load_balancing_config.load_balancing_kind = Some(LoadBalancingKind::RoundRobin);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_request_timeout(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    timeout_ms: cass_uint64_t,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();
    profile_builder.modify_in_place(|builder| {
        builder.request_timeout(Some(std::time::Duration::from_millis(timeout_ms)))
    });

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_retry_policy(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    retry_policy: CassBorrowedPtr<CassRetryPolicy>,
) -> CassError {
    let retry_policy: Arc<dyn RetryPolicy> = match ArcFFI::as_ref(retry_policy).unwrap() {
        DefaultRetryPolicy(default) => Arc::clone(default) as _,
        FallthroughRetryPolicy(fallthrough) => Arc::clone(fallthrough) as _,
        DowngradingConsistencyRetryPolicy(downgrading) => Arc::clone(downgrading) as _,
    };
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();
    profile_builder.modify_in_place(|builder| builder.retry_policy(retry_policy));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_serial_consistency(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    serial_consistency: CassConsistency,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();

    let maybe_serial_consistency =
        if serial_consistency == CassConsistency::CASS_CONSISTENCY_UNKNOWN {
            None
        } else {
            match serial_consistency.try_into() {
                Ok(c) => Some(c),
                Err(_) => return CassError::CASS_ERROR_LIB_BAD_PARAMS,
            }
        };
    profile_builder.modify_in_place(|builder| builder.serial_consistency(maybe_serial_consistency));

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_token_aware_routing(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    enabled: cass_bool_t,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();
    profile_builder
        .load_balancing_config
        .token_awareness_enabled = enabled != 0;

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_execution_profile_set_token_aware_routing_shuffle_replicas(
    profile: CassBorrowedMutPtr<CassExecProfile>,
    enabled: cass_bool_t,
) -> CassError {
    let profile_builder = BoxFFI::as_mut_ref(profile).unwrap();
    profile_builder
        .load_balancing_config
        .token_aware_shuffling_replicas_enabled = enabled != 0;

    CassError::CASS_OK
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::assert_cass_error_eq;
    use crate::{
        argconv::{make_c_str, str_to_c_str_n},
        batch::{cass_batch_add_statement, cass_batch_free, cass_batch_new},
        cass_types::CassBatchType,
        statement::{cass_statement_free, cass_statement_new},
    };

    use assert_matches::assert_matches;

    #[test]
    #[ntest::timeout(100)]
    fn test_exec_profile_name() {
        use std::convert::TryInto;
        let empty = "".to_owned();
        let nonempty = "a".to_owned();
        assert_eq!(
            empty.try_into() as Result<ExecProfileName, _>,
            Err(EmptyProfileName)
        );
        assert_eq!(
            nonempty.clone().try_into() as Result<ExecProfileName, _>,
            Ok(ExecProfileName(nonempty))
        );
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_load_balancing_config() {
        unsafe {
            let mut profile_raw = cass_execution_profile_new();
            {
                /* Test valid configurations */
                {
                    let profile = BoxFFI::as_ref(profile_raw.borrow()).unwrap();
                    assert_matches!(profile.load_balancing_config.load_balancing_kind, None);
                    assert!(profile.load_balancing_config.token_awareness_enabled);
                    assert!(!profile.load_balancing_config.latency_awareness_enabled);
                }
                {
                    cass_execution_profile_set_token_aware_routing(profile_raw.borrow_mut(), 0);
                    assert_cass_error_eq!(
                        cass_execution_profile_set_load_balance_dc_aware(
                            profile_raw.borrow_mut(),
                            c"eu".as_ptr(),
                            0,
                            0
                        ),
                        CassError::CASS_OK
                    );
                    cass_execution_profile_set_latency_aware_routing(profile_raw.borrow_mut(), 1);
                    // These values cannot currently be tested to be set properly in the latency awareness builder,
                    // but at least we test that the function completed successfully.
                    cass_execution_profile_set_latency_aware_routing_settings(
                        profile_raw.borrow_mut(),
                        2.,
                        1,
                        2000,
                        100,
                        40,
                    );

                    let profile = BoxFFI::as_ref(profile_raw.borrow()).unwrap();
                    let load_balancing_kind = &profile.load_balancing_config.load_balancing_kind;
                    match load_balancing_kind {
                        Some(LoadBalancingKind::DcAware { local_dc }) => {
                            assert_eq!(local_dc, "eu")
                        }
                        _ => panic!("Expected preferred dc"),
                    }
                    assert!(!profile.load_balancing_config.token_awareness_enabled);
                    assert!(profile.load_balancing_config.latency_awareness_enabled);
                }
                /* Test invalid configurations */
                {
                    // Nonzero deprecated parameters
                    assert_cass_error_eq!(
                        cass_execution_profile_set_load_balance_dc_aware(
                            profile_raw.borrow_mut(),
                            c"eu".as_ptr(),
                            1,
                            0
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                    assert_cass_error_eq!(
                        cass_execution_profile_set_load_balance_dc_aware(
                            profile_raw.borrow_mut(),
                            c"eu".as_ptr(),
                            0,
                            1
                        ),
                        CassError::CASS_ERROR_LIB_BAD_PARAMS
                    );
                }
            }

            cass_execution_profile_free(profile_raw);
        }
    }

    impl PerStatementExecProfile {
        pub(crate) fn inner(&self) -> &Arc<RwLock<PerStatementExecProfileInner>> {
            &self.0
        }
    }

    impl PerStatementExecProfileInner {
        pub(crate) fn as_name(&self) -> Option<&ExecProfileName> {
            if let PerStatementExecProfileInner::Unresolved(name) = self {
                Some(name)
            } else {
                None
            }
        }

        pub(crate) fn as_handle(&self) -> Option<&ExecutionProfileHandle> {
            if let PerStatementExecProfileInner::Resolved(profile) = self {
                Some(profile)
            } else {
                None
            }
        }
    }

    #[test]
    #[ntest::timeout(100)]
    fn test_statement_and_batch_set_exec_profile() {
        unsafe {
            let empty_query = make_c_str!("");
            let mut statement_raw = cass_statement_new(empty_query, 0);
            let mut batch_raw = cass_batch_new(CassBatchType::CASS_BATCH_TYPE_LOGGED);
            assert_cass_error_eq!(
                cass_batch_add_statement(batch_raw.borrow_mut(), statement_raw.borrow()),
                CassError::CASS_OK
            );

            {
                /* Test valid configurations */
                {
                    let statement = BoxFFI::as_ref(statement_raw.borrow()).unwrap();
                    let batch = BoxFFI::as_ref(batch_raw.borrow()).unwrap();
                    assert!(statement.exec_profile.is_none());
                    assert!(batch.exec_profile.is_none());
                }
                {
                    let valid_name = "profile";
                    let valid_name_c_str = make_c_str!("profile");
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile(
                            statement_raw.borrow_mut(),
                            valid_name_c_str,
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(batch_raw.borrow_mut(), valid_name_c_str,),
                        CassError::CASS_OK
                    );

                    let statement = BoxFFI::as_ref(statement_raw.borrow()).unwrap();
                    let batch = BoxFFI::as_ref(batch_raw.borrow()).unwrap();
                    assert_eq!(
                        statement
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .0
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &valid_name.to_owned().try_into().unwrap()
                    );
                    assert_eq!(
                        batch
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .0
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &valid_name.to_owned().try_into().unwrap()
                    );
                }
                {
                    // NULL name sets exec profile to None
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile(
                            statement_raw.borrow_mut(),
                            std::ptr::null::<i8>()
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(
                            batch_raw.borrow_mut(),
                            std::ptr::null::<i8>()
                        ),
                        CassError::CASS_OK
                    );

                    let statement = BoxFFI::as_ref(statement_raw.borrow()).unwrap();
                    let batch = BoxFFI::as_ref(batch_raw.borrow()).unwrap();
                    assert!(statement.exec_profile.is_none());
                    assert!(batch.exec_profile.is_none());
                }
                {
                    // valid name again, this time using `..._n` setter
                    let valid_name = "profile1";
                    let (valid_name_c_str, valid_name_len) = str_to_c_str_n(valid_name);
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile_n(
                            statement_raw.borrow_mut(),
                            valid_name_c_str,
                            valid_name_len,
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile_n(
                            batch_raw.borrow_mut(),
                            valid_name_c_str,
                            valid_name_len,
                        ),
                        CassError::CASS_OK
                    );

                    let statement = BoxFFI::as_ref(statement_raw.borrow()).unwrap();
                    let batch = BoxFFI::as_ref(batch_raw.borrow()).unwrap();
                    assert_eq!(
                        statement
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .0
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &valid_name.to_owned().try_into().unwrap()
                    );
                    assert_eq!(
                        batch
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .0
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &valid_name.to_owned().try_into().unwrap()
                    );
                }
                {
                    // empty name sets exec profile to None
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile(
                            statement_raw.borrow_mut(),
                            make_c_str!("")
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(batch_raw.borrow_mut(), make_c_str!("")),
                        CassError::CASS_OK
                    );

                    let statement = BoxFFI::as_ref(statement_raw.borrow()).unwrap();
                    let batch = BoxFFI::as_ref(batch_raw.borrow()).unwrap();
                    assert!(statement.exec_profile.is_none());
                    assert!(batch.exec_profile.is_none());
                }
            }

            cass_statement_free(statement_raw);
            cass_batch_free(batch_raw);
        }
    }
}
