use crate::argconv::*;
use crate::batch::CassBatch;
use crate::cass_error::*;
use crate::cass_types::{CassDataType, CassDataTypeInner, UDTDataType};
use crate::cluster::build_session_builder;
use crate::cluster::CassCluster;
use crate::exec_profile::{CassExecProfile, ExecProfileName, PerStatementExecProfile};
use crate::future::{CassFuture, CassFutureResult, CassResultValue};
use crate::metadata::create_table_metadata;
use crate::metadata::{CassKeyspaceMeta, CassMaterializedViewMeta, CassSchemaMeta};
use crate::prepared::CassPrepared;
use crate::query_result::{CassResult, CassResultKind, CassResultMetadata};
use crate::statement::{BoundStatement, CassStatement, SimpleQueryRowSerializer};
use crate::types::{cass_uint64_t, size_t};
use crate::uuid::CassUuid;
use scylla::frame::types::Consistency;
use scylla::query::Query;
use scylla::transport::errors::QueryError;
use scylla::transport::execution_profile::ExecutionProfileHandle;
use scylla::transport::PagingStateResponse;
use scylla::{QueryResult, Session, SessionBuilder};
use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use std::os::raw::c_char;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub struct CassSessionInner {
    session: Session,
    exec_profile_map: HashMap<ExecProfileName, ExecutionProfileHandle>,
    client_id: uuid::Uuid,
}

impl CassSessionInner {
    pub(crate) fn resolve_exec_profile(
        &self,
        name: &ExecProfileName,
    ) -> Result<&ExecutionProfileHandle, (CassError, String)> {
        // Empty name means no execution profile set.
        self.exec_profile_map.get(name).ok_or_else(|| {
            (
                CassError::CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID,
                format!("{} does not exist", name.deref()),
            )
        })
    }

    // Clippy claims it is possible to make this `async fn`, but it's terribly wrong,
    // because async fn can't have its future bound to a specific lifetime, which is
    // required in this case.
    #[allow(clippy::manual_async_fn)]
    fn get_or_resolve_profile_handle<'a>(
        &'a self,
        exec_profile: Option<&'a PerStatementExecProfile>,
    ) -> impl Future<Output = Result<Option<ExecutionProfileHandle>, (CassError, String)>> + 'a
    {
        async move {
            if let Some(profile) = exec_profile {
                let handle = profile.get_or_resolve_profile_handle(self).await?;
                Ok(Some(handle))
            } else {
                Ok(None)
            }
        }
    }

    fn connect(
        // This reference is 'static because this is the only was of assuring the borrow checker
        // that holding it in our returned future is sound. Ideally, we would prefer to have
        // the returned future's lifetime constrained by real lifetime of the session's RwLock,
        // but this is impossible to be guaranteed due to C/Rust cross-language barrier.
        session_opt: &'static RwLock<Option<CassSessionInner>>,
        cluster: &CassCluster,
        keyspace: Option<String>,
    ) -> CassSharedPtr<CassFuture> {
        let session_builder = build_session_builder(cluster);
        let exec_profile_map = cluster.execution_profile_map().clone();

        CassFuture::make_raw(Self::connect_fut(
            session_opt,
            session_builder,
            exec_profile_map,
            cluster
                .get_client_id()
                // If user did not set a client id, generate a random uuid v4.
                .unwrap_or_else(uuid::Uuid::new_v4),
            keyspace,
        ))
    }

    async fn connect_fut(
        session_opt: &RwLock<Option<CassSessionInner>>,
        session_builder_fut: impl Future<Output = SessionBuilder>,
        exec_profile_builder_map: HashMap<ExecProfileName, CassExecProfile>,
        client_id: uuid::Uuid,
        keyspace: Option<String>,
    ) -> CassFutureResult {
        // This can sleep for a long time, but only if someone connects/closes session
        // from more than 1 thread concurrently, which is inherently stupid thing to do.
        let mut session_guard = session_opt.write().await;
        if session_guard.is_some() {
            return Err((
                CassError::CASS_ERROR_LIB_UNABLE_TO_CONNECT,
                "Already connecting, closing, or connected".msg(),
            ));
        }

        let mut session_builder = session_builder_fut.await;
        let default_profile = session_builder
            .config
            .default_execution_profile_handle
            .to_profile();

        let mut exec_profile_map = HashMap::with_capacity(exec_profile_builder_map.len());
        for (name, builder) in exec_profile_builder_map {
            exec_profile_map.insert(name, builder.build(&default_profile).await.into_handle());
        }

        if let Some(keyspace) = keyspace {
            session_builder = session_builder.use_keyspace(keyspace, false);
        }

        let session = session_builder
            .build()
            .await
            .map_err(|err| (err.to_cass_error(), err.msg()))?;

        *session_guard = Some(CassSessionInner {
            session,
            exec_profile_map,
            client_id,
        });
        Ok(CassResultValue::Empty)
    }
}

pub type CassSession = RwLock<Option<CassSessionInner>>;

impl FFI for CassSession {
    type Ownership = OwnershipShared;
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_new() -> CassSharedPtr<CassSession> {
    let session = Arc::new(RwLock::new(None::<CassSessionInner>));
    ArcFFI::into_ptr(session)
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_connect(
    session_raw: CassSharedPtr<CassSession>,
    cluster_raw: CassExclusiveConstPtr<CassCluster>,
) -> CassSharedPtr<CassFuture> {
    let session_opt = ArcFFI::into_ref(session_raw).unwrap();
    let cluster: &CassCluster = BoxFFI::as_ref(&cluster_raw).unwrap();

    CassSessionInner::connect(session_opt, cluster, None)
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_connect_keyspace(
    session_raw: CassSharedPtr<CassSession>,
    cluster_raw: CassExclusiveConstPtr<CassCluster>,
    keyspace: *const c_char,
) -> CassSharedPtr<CassFuture> {
    cass_session_connect_keyspace_n(session_raw, cluster_raw, keyspace, strlen(keyspace))
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_connect_keyspace_n(
    session_raw: CassSharedPtr<CassSession>,
    cluster_raw: CassExclusiveConstPtr<CassCluster>,
    keyspace: *const c_char,
    keyspace_length: size_t,
) -> CassSharedPtr<CassFuture> {
    let session_opt = ArcFFI::into_ref(session_raw).unwrap();
    let cluster: &CassCluster = BoxFFI::as_ref(&cluster_raw).unwrap();
    let keyspace = ptr_to_cstr_n(keyspace, keyspace_length).map(ToOwned::to_owned);

    CassSessionInner::connect(session_opt, cluster, keyspace)
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_execute_batch(
    session_raw: CassSharedPtr<CassSession>,
    batch_raw: CassExclusiveConstPtr<CassBatch>,
) -> CassSharedPtr<CassFuture> {
    let session_opt = ArcFFI::into_ref(session_raw).unwrap();
    let batch_from_raw = BoxFFI::as_ref(&batch_raw).unwrap();
    let mut state = batch_from_raw.state.clone();
    let request_timeout_ms = batch_from_raw.batch_request_timeout_ms;

    // DO NOT refer to `batch_from_raw` inside the async block, as I've done just to face a segfault.
    let batch_exec_profile = batch_from_raw.exec_profile.clone();
    #[allow(unused, clippy::let_unit_value)]
    let batch_from_raw = (); // Hardening shadow to avoid use-after-free.

    let future = async move {
        let session_guard = session_opt.read().await;
        if session_guard.is_none() {
            return Err((
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                "Session is not connected".msg(),
            ));
        }

        let cass_session_inner = &session_guard.as_ref().unwrap();
        let session = &cass_session_inner.session;

        let handle = cass_session_inner
            .get_or_resolve_profile_handle(batch_exec_profile.as_ref())
            .await?;

        Arc::make_mut(&mut state)
            .batch
            .set_execution_profile_handle(handle);

        let query_res = session.batch(&state.batch, &state.bound_values).await;
        match query_res {
            Ok(_result) => Ok(CassResultValue::QueryResult(Arc::new(CassResult {
                tracing_id: None,
                paging_state_response: PagingStateResponse::NoMorePages,
                kind: CassResultKind::NonRows,
            }))),
            Err(err) => Ok(CassResultValue::QueryError(Arc::new(err.into()))),
        }
    };

    match request_timeout_ms {
        Some(timeout_ms) => {
            CassFuture::make_raw(async move { request_with_timeout(timeout_ms, future).await })
        }
        None => CassFuture::make_raw(future),
    }
}

async fn request_with_timeout(
    request_timeout_ms: cass_uint64_t,
    future: impl Future<Output = Result<CassResultValue, (CassError, String)>>,
) -> Result<CassResultValue, (CassError, String)> {
    match tokio::time::timeout(Duration::from_millis(request_timeout_ms), future).await {
        Ok(result) => result,
        Err(_timeout_err) => Ok(CassResultValue::QueryError(Arc::new(
            QueryError::TimeoutError.into(),
        ))),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_execute(
    session_raw: CassSharedPtr<CassSession>,
    statement_raw: CassExclusiveConstPtr<CassStatement>,
) -> CassSharedPtr<CassFuture> {
    let session_opt = ArcFFI::into_ref(session_raw).unwrap();

    // DO NOT refer to `statement_opt` inside the async block, as I've done just to face a segfault.
    let statement_opt = BoxFFI::as_ref(&statement_raw).unwrap();
    let paging_state = statement_opt.paging_state.clone();
    let paging_enabled = statement_opt.paging_enabled;
    let request_timeout_ms = statement_opt.request_timeout_ms;

    let mut statement = statement_opt.statement.clone();
    let statement_exec_profile = statement_opt.exec_profile.clone();
    #[allow(unused, clippy::let_unit_value)]
    let statement_opt = (); // Hardening shadow to avoid use-after-free.

    let future = async move {
        let session_guard = session_opt.read().await;
        if session_guard.is_none() {
            return Err((
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                "Session is not connected".msg(),
            ));
        }
        let cass_session_inner = session_guard.as_ref().unwrap();
        let session = &cass_session_inner.session;

        let handle = cass_session_inner
            .get_or_resolve_profile_handle(statement_exec_profile.as_ref())
            .await?;

        match &mut statement {
            BoundStatement::Simple(query) => query.query.set_execution_profile_handle(handle),
            BoundStatement::Prepared(prepared) => Arc::make_mut(&mut prepared.statement)
                .statement
                .set_execution_profile_handle(handle),
        }

        // Creating a type alias here to fix clippy lints.
        // I want this type to be explicit, so future developers can understand
        // what's going on here (and why we include some weird Option of data types).
        type QueryRes = Result<
            (
                QueryResult,
                PagingStateResponse,
                // We unfortunately have to retrieve the metadata here.
                // Since `query.query` is consumed, we cannot match the statement
                // after execution, to retrieve the cached metadata in case
                // of prepared statements.
                Option<Arc<CassResultMetadata>>,
            ),
            QueryError,
        >;
        let query_res: QueryRes = match statement {
            BoundStatement::Simple(query) => {
                // We don't store result metadata for Queries - return None.
                let maybe_result_metadata = None;

                let bound_values = SimpleQueryRowSerializer {
                    bound_values: query.bound_values,
                    name_to_bound_index: query.name_to_bound_index,
                };

                if paging_enabled {
                    session
                        .query_single_page(query.query, bound_values, paging_state)
                        .await
                        .map(|(qr, psr)| (qr, psr, maybe_result_metadata))
                } else {
                    session
                        .query_unpaged(query.query, bound_values)
                        .await
                        .map(|result| {
                            (
                                result,
                                PagingStateResponse::NoMorePages,
                                maybe_result_metadata,
                            )
                        })
                }
            }
            BoundStatement::Prepared(prepared) => {
                // Clone result metadata, so we don't need to construct it from scratch in
                // `CassResultMetadata::from_column_specs` - it requires a lot of allocations for complex types.
                let maybe_result_metadata = Some(Arc::clone(&prepared.statement.result_metadata));

                if paging_enabled {
                    session
                        .execute_single_page(
                            &prepared.statement.statement,
                            prepared.bound_values,
                            paging_state,
                        )
                        .await
                        .map(|(qr, psr)| (qr, psr, maybe_result_metadata))
                } else {
                    session
                        .execute_unpaged(&prepared.statement.statement, prepared.bound_values)
                        .await
                        .map(|result| {
                            (
                                result,
                                PagingStateResponse::NoMorePages,
                                maybe_result_metadata,
                            )
                        })
                }
            }
        };

        match query_res {
            Ok((result, paging_state_response, maybe_result_metadata)) => {
                match CassResult::from_result_payload(
                    result,
                    paging_state_response,
                    maybe_result_metadata,
                ) {
                    Ok(result) => Ok(CassResultValue::QueryResult(Arc::new(result))),
                    Err(e) => Ok(CassResultValue::QueryError(Arc::new(e))),
                }
            }
            Err(err) => Ok(CassResultValue::QueryError(Arc::new(err.into()))),
        }
    };

    match request_timeout_ms {
        Some(timeout_ms) => {
            CassFuture::make_raw(async move { request_with_timeout(timeout_ms, future).await })
        }
        None => CassFuture::make_raw(future),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare_from_existing(
    cass_session: CassSharedPtr<CassSession>,
    statement: CassExclusiveConstPtr<CassStatement>,
) -> CassSharedPtr<CassFuture> {
    let session = ArcFFI::into_ref(cass_session).unwrap();
    let cass_statement = BoxFFI::as_ref(&statement).unwrap();
    let statement = cass_statement.statement.clone();

    CassFuture::make_raw(async move {
        let query = match &statement {
            BoundStatement::Simple(q) => q,
            BoundStatement::Prepared(ps) => {
                return Ok(CassResultValue::Prepared(ps.statement.clone()));
            }
        };

        let session_guard = session.read().await;
        if session_guard.is_none() {
            return Err((
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                "Session is not connected".msg(),
            ));
        }
        let session = &session_guard.as_ref().unwrap().session;
        let prepared = session
            .prepare(query.query.clone())
            .await
            .map_err(|err| (err.to_cass_error(), err.msg()))?;

        Ok(CassResultValue::Prepared(Arc::new(
            CassPrepared::new_from_prepared_statement(prepared),
        )))
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare(
    session: CassSharedPtr<CassSession>,
    query: *const c_char,
) -> CassSharedPtr<CassFuture> {
    cass_session_prepare_n(session, query, strlen(query))
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare_n(
    cass_session_raw: CassSharedPtr<CassSession>,
    query: *const c_char,
    query_length: size_t,
) -> CassSharedPtr<CassFuture> {
    let query_str = ptr_to_cstr_n(query, query_length)
        // Apparently nullptr denotes an empty statement string.
        // It seems to be intended (for some weird reason, why not save a round-trip???)
        // to receive a server error in such case (CASS_ERROR_SERVER_SYNTAX_ERROR).
        // There is a test for this: `NullStringApiArgsTest.Integration_Cassandra_PrepareNullQuery`.
        .unwrap_or_default();
    let query = Query::new(query_str.to_string());
    let cass_session = ArcFFI::into_ref(cass_session_raw).unwrap();

    CassFuture::make_raw(async move {
        let session_guard = cass_session.read().await;
        if session_guard.is_none() {
            return Err((
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                "Session is not connected".msg(),
            ));
        }
        let session = &session_guard.as_ref().unwrap().session;

        let mut prepared = session
            .prepare(query)
            .await
            .map_err(|err| (err.to_cass_error(), err.msg()))?;

        // Set Cpp Driver default configuration for queries:
        prepared.set_consistency(Consistency::One);

        Ok(CassResultValue::Prepared(Arc::new(
            CassPrepared::new_from_prepared_statement(prepared),
        )))
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_free(session_raw: CassSharedPtr<CassSession>) {
    ArcFFI::free(session_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_close(
    session: CassSharedPtr<CassSession>,
) -> CassSharedPtr<CassFuture> {
    let session_opt = ArcFFI::into_ref(session).unwrap();

    CassFuture::make_raw(async move {
        let mut session_guard = session_opt.write().await;
        if session_guard.is_none() {
            return Err((
                CassError::CASS_ERROR_LIB_UNABLE_TO_CLOSE,
                "Already closing or closed".msg(),
            ));
        }

        *session_guard = None;

        Ok(CassResultValue::Empty)
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_get_client_id(
    session: CassSharedPtr<CassSession>,
) -> CassUuid {
    let cass_session = ArcFFI::as_ref(&session).unwrap();

    let client_id: uuid::Uuid = cass_session.blocking_read().as_ref().unwrap().client_id;
    client_id.into()
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_get_schema_meta(
    session: CassSharedPtr<CassSession>,
) -> CassExclusiveConstPtr<CassSchemaMeta> {
    let cass_session = ArcFFI::as_ref(&session).unwrap();
    let mut keyspaces: HashMap<String, CassKeyspaceMeta> = HashMap::new();

    for (keyspace_name, keyspace) in cass_session
        .blocking_read()
        .as_ref()
        .unwrap()
        .session
        .get_cluster_data()
        .get_keyspace_info()
    {
        let mut user_defined_type_data_type = HashMap::new();
        let mut tables = HashMap::new();
        let mut views = HashMap::new();

        for udt_name in keyspace.user_defined_types.keys() {
            user_defined_type_data_type.insert(
                udt_name.clone(),
                CassDataType::new_arced(CassDataTypeInner::UDT(UDTDataType::create_with_params(
                    &keyspace.user_defined_types,
                    keyspace_name,
                    udt_name,
                    false,
                ))),
            );
        }

        for (table_name, table_metadata) in &keyspace.tables {
            let cass_table_meta_arced = Arc::new_cyclic(|weak_cass_table_meta| {
                let mut cass_table_meta = create_table_metadata(
                    keyspace_name,
                    table_name,
                    table_metadata,
                    &keyspace.user_defined_types,
                );

                let mut table_views = HashMap::new();
                for (view_name, view_metadata) in &keyspace.views {
                    let cass_view_table_meta = create_table_metadata(
                        keyspace_name,
                        view_name,
                        &view_metadata.view_metadata,
                        &keyspace.user_defined_types,
                    );
                    let cass_view_meta = CassMaterializedViewMeta {
                        name: view_name.clone(),
                        view_metadata: cass_view_table_meta,
                        base_table: weak_cass_table_meta.clone(),
                    };
                    let cass_view_meta_arced = Arc::new(cass_view_meta);
                    table_views.insert(view_name.clone(), cass_view_meta_arced.clone());

                    views.insert(view_name.clone(), cass_view_meta_arced);
                }

                cass_table_meta.views = table_views;

                cass_table_meta
            });

            tables.insert(table_name.clone(), cass_table_meta_arced);
        }

        keyspaces.insert(
            keyspace_name.clone(),
            CassKeyspaceMeta {
                name: keyspace_name.clone(),
                user_defined_type_data_type,
                tables,
                views,
            },
        );
    }

    BoxFFI::into_ptr(Box::new(CassSchemaMeta { keyspaces }))
}

#[cfg(test)]
mod tests {
    use rusty_fork::rusty_fork_test;
    use scylla::transport::errors::DbError;
    use scylla_proxy::{
        Condition, Node, Proxy, Reaction, RequestFrame, RequestOpcode, RequestReaction,
        RequestRule, ResponseFrame, RunningProxy,
    };
    use tracing::instrument::WithSubscriber;

    use super::*;
    use crate::{
        argconv::make_c_str,
        batch::{
            cass_batch_add_statement, cass_batch_free, cass_batch_new, cass_batch_set_retry_policy,
        },
        cass_types::CassBatchType,
        cluster::{
            cass_cluster_free, cass_cluster_new, cass_cluster_set_contact_points_n,
            cass_cluster_set_execution_profile, cass_cluster_set_latency_aware_routing,
            cass_cluster_set_retry_policy,
        },
        exec_profile::{
            cass_batch_set_execution_profile, cass_batch_set_execution_profile_n,
            cass_execution_profile_free, cass_execution_profile_new,
            cass_execution_profile_set_latency_aware_routing,
            cass_execution_profile_set_retry_policy, cass_statement_set_execution_profile,
            cass_statement_set_execution_profile_n, ExecProfileName,
        },
        future::{
            cass_future_error_code, cass_future_error_message, cass_future_free, cass_future_wait,
        },
        retry_policy::{
            cass_retry_policy_default_new, cass_retry_policy_fallthrough_new, CassRetryPolicy,
        },
        statement::{cass_statement_free, cass_statement_new, cass_statement_set_retry_policy},
        testing::assert_cass_error_eq,
        types::cass_bool_t,
    };
    use std::{
        collections::HashSet,
        convert::{TryFrom, TryInto},
        iter,
        net::SocketAddr,
    };

    // This is for convenient logs from failing tests. Just call it at the beginning of a test.
    #[allow(unused)]
    fn init_logger() {
        let _ = tracing_subscriber::fmt::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .without_time()
            .try_init();
    }

    unsafe fn cass_future_wait_check_and_free(fut: CassSharedPtr<CassFuture>) {
        cass_future_wait(fut.clone());
        if cass_future_error_code(fut.clone()) != CassError::CASS_OK {
            let mut message: *const c_char = std::ptr::null();
            let mut message_len: size_t = 0;
            cass_future_error_message(fut.clone(), &mut message, &mut message_len);
            eprintln!("{:?}", ptr_to_cstr_n(message, message_len));
        }
        assert_cass_error_eq!(cass_future_error_code(fut.clone()), CassError::CASS_OK);
        cass_future_free(fut);
    }

    fn handshake_rules() -> impl IntoIterator<Item = RequestRule> {
        [
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Options),
                RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                    ResponseFrame::forged_supported(frame.params, &HashMap::new()).unwrap()
                })),
            ),
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Startup)
                    .or(Condition::RequestOpcode(RequestOpcode::Register)),
                RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                    ResponseFrame::forged_ready(frame.params)
                })),
            ),
        ]
    }

    // As these are very generic, they should be put last in the rules Vec.
    fn generic_drop_queries_rules() -> impl IntoIterator<Item = RequestRule> {
        [RequestRule(
            Condition::RequestOpcode(RequestOpcode::Query),
            // We won't respond to any queries (including metadata fetch),
            // but the driver will manage to continue with dummy metadata.
            RequestReaction::forge().server_error(),
        )]
    }

    pub(crate) async fn test_with_one_proxy_one(
        test: impl FnOnce(SocketAddr, RunningProxy) -> RunningProxy + Send + 'static,
        rules: impl IntoIterator<Item = RequestRule>,
    ) {
        let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);

        let proxy = Proxy::builder()
            .with_node(
                Node::builder()
                    .proxy_address(proxy_addr)
                    .request_rules(rules.into_iter().collect())
                    .build_dry_mode(),
            )
            .build()
            .run()
            .await
            .unwrap();

        // This is required to avoid the clash of a runtime built inside another runtime
        // (the test runs one runtime to drive the proxy, and CassFuture implementation uses another)
        let proxy = tokio::task::spawn_blocking(move || test(proxy_addr, proxy))
            .await
            .expect("Test thread panicked");

        let _ = proxy.finish().await;
    }

    #[tokio::test]
    #[ntest::timeout(5000)]
    async fn session_clones_and_freezes_exec_profiles_mapping() {
        init_logger();
        test_with_one_proxy_one(
            session_clones_and_freezes_exec_profiles_mapping_do,
            handshake_rules()
                .into_iter()
                .chain(generic_drop_queries_rules()),
        )
        .with_current_subscriber()
        .await;
    }

    fn session_clones_and_freezes_exec_profiles_mapping_do(
        node_addr: SocketAddr,
        proxy: RunningProxy,
    ) -> RunningProxy {
        unsafe {
            let cluster_raw = cass_cluster_new();
            let ip = node_addr.ip().to_string();
            let (c_ip, c_ip_len) = str_to_c_str_n(ip.as_str());

            assert_cass_error_eq!(
                cass_cluster_set_contact_points_n(cluster_raw.clone(), c_ip, c_ip_len),
                CassError::CASS_OK
            );
            let session_raw = cass_session_new();
            let profile_raw = cass_execution_profile_new();
            {
                cass_future_wait_check_and_free(cass_session_connect(
                    session_raw.clone(),
                    cluster_raw.clone().into_const(),
                ));
                // Initially, the profile map is empty.

                assert!(ArcFFI::as_ref(&session_raw)
                    .unwrap()
                    .blocking_read()
                    .as_ref()
                    .unwrap()
                    .exec_profile_map
                    .is_empty());

                cass_cluster_set_execution_profile(
                    cluster_raw.clone(),
                    make_c_str!("prof"),
                    profile_raw.clone().into_const(),
                );
                // Mutations in cluster do not affect the session that was connected before.
                assert!(ArcFFI::as_ref(&session_raw)
                    .unwrap()
                    .blocking_read()
                    .as_ref()
                    .unwrap()
                    .exec_profile_map
                    .is_empty());

                cass_future_wait_check_and_free(cass_session_close(session_raw.clone()));

                // Mutations in cluster are now propagated to the session.
                cass_future_wait_check_and_free(cass_session_connect(
                    session_raw.clone(),
                    cluster_raw.clone().into_const(),
                ));
                let profile_map_keys = ArcFFI::as_ref(&session_raw)
                    .unwrap()
                    .blocking_read()
                    .as_ref()
                    .unwrap()
                    .exec_profile_map
                    .keys()
                    .cloned()
                    .collect::<HashSet<_>>();
                assert_eq!(
                    profile_map_keys,
                    std::iter::once(ExecProfileName::try_from("prof".to_owned()).unwrap())
                        .collect::<HashSet<_>>()
                );
                cass_future_wait_check_and_free(cass_session_close(session_raw.clone()));
            }
            cass_execution_profile_free(profile_raw);
            cass_session_free(session_raw);
            cass_cluster_free(cluster_raw);
        }
        proxy
    }

    #[tokio::test]
    #[ntest::timeout(5000)]
    async fn session_resolves_exec_profile_on_first_query() {
        init_logger();
        test_with_one_proxy_one(
            session_resolves_exec_profile_on_first_query_do,
            handshake_rules().into_iter().chain(
                iter::once(RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Query)
                        .or(Condition::RequestOpcode(RequestOpcode::Batch))
                        .and(Condition::BodyContainsCaseInsensitive(Box::new(
                            *b"INSERT INTO system.",
                        ))),
                    // We simulate the write failure error that a Scylla node would respond with anyway.
                    RequestReaction::forge().write_failure(),
                ))
                .chain(generic_drop_queries_rules()),
            ),
        )
        .with_current_subscriber()
        .await;
    }

    fn session_resolves_exec_profile_on_first_query_do(
        node_addr: SocketAddr,
        proxy: RunningProxy,
    ) -> RunningProxy {
        unsafe {
            let cluster_raw = cass_cluster_new();
            let ip = node_addr.ip().to_string();
            let (c_ip, c_ip_len) = str_to_c_str_n(ip.as_str());

            assert_cass_error_eq!(
                cass_cluster_set_contact_points_n(cluster_raw.clone(), c_ip, c_ip_len),
                CassError::CASS_OK
            );

            let session_raw = cass_session_new();

            let profile_raw = cass_execution_profile_new();
            // A name of a profile that will have been registered in the Cluster.
            let valid_name = "profile";
            let valid_name_c_str = make_c_str!("profile");
            // A name of a profile that won't have been registered in the Cluster.
            let nonexisting_name = "profile1";
            let (nonexisting_name_c_str, nonexisting_name_len) = str_to_c_str_n(nonexisting_name);

            // Inserting into virtual system tables is prohibited and results in WriteFailure error.
            let invalid_query = make_c_str!("INSERT INTO system.runtime_info (group, item, value) VALUES ('bindings_test', 'bindings_test', 'bindings_test')");
            let statement_raw = cass_statement_new(invalid_query, 0);
            let batch_raw = cass_batch_new(CassBatchType::CASS_BATCH_TYPE_LOGGED);
            assert_cass_error_eq!(
                cass_batch_add_statement(batch_raw.clone(), statement_raw.clone().into_const()),
                CassError::CASS_OK
            );

            assert_cass_error_eq!(
                cass_cluster_set_execution_profile(
                    cluster_raw.clone(),
                    valid_name_c_str,
                    profile_raw.clone().into_const(),
                ),
                CassError::CASS_OK
            );

            cass_future_wait_check_and_free(cass_session_connect(
                session_raw.clone(),
                cluster_raw.clone().into_const(),
            ));
            {
                /* Test valid configurations */
                let statement = BoxFFI::as_ref(&statement_raw).unwrap();
                let batch = BoxFFI::as_ref(&batch_raw).unwrap();
                {
                    assert!(statement.exec_profile.is_none());
                    assert!(batch.exec_profile.is_none());

                    // Set exec profile - it is not yet resolved.
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile(
                            statement_raw.clone(),
                            valid_name_c_str,
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(batch_raw.clone(), valid_name_c_str,),
                        CassError::CASS_OK
                    );
                    assert_eq!(
                        statement
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .inner()
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
                            .inner()
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &valid_name.to_owned().try_into().unwrap()
                    );

                    // Make a query - this should resolve the profile.
                    assert_cass_error_eq!(
                        cass_future_error_code(cass_session_execute(
                            session_raw.clone(),
                            statement_raw.clone().into_const()
                        )),
                        CassError::CASS_ERROR_SERVER_WRITE_FAILURE
                    );
                    assert!(statement
                        .exec_profile
                        .as_ref()
                        .unwrap()
                        .inner()
                        .read()
                        .unwrap()
                        .as_handle()
                        .is_some());
                    assert_cass_error_eq!(
                        cass_future_error_code(cass_session_execute_batch(
                            session_raw.clone(),
                            batch_raw.clone().into_const(),
                        )),
                        CassError::CASS_ERROR_SERVER_WRITE_FAILURE
                    );
                    assert!(batch
                        .exec_profile
                        .as_ref()
                        .unwrap()
                        .inner()
                        .read()
                        .unwrap()
                        .as_handle()
                        .is_some());

                    // NULL name sets exec profile to None
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile(
                            statement_raw.clone(),
                            std::ptr::null::<i8>()
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(batch_raw.clone(), std::ptr::null::<i8>()),
                        CassError::CASS_OK
                    );
                    assert!(statement.exec_profile.is_none());
                    assert!(batch.exec_profile.is_none());

                    // valid name again, but of nonexisting profile!
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile_n(
                            statement_raw.clone(),
                            nonexisting_name_c_str,
                            nonexisting_name_len,
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile_n(
                            batch_raw.clone(),
                            nonexisting_name_c_str,
                            nonexisting_name_len,
                        ),
                        CassError::CASS_OK
                    );
                    assert_eq!(
                        statement
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .inner()
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &nonexisting_name.to_owned().try_into().unwrap()
                    );
                    assert_eq!(
                        batch
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .inner()
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &nonexisting_name.to_owned().try_into().unwrap()
                    );

                    // So when we now issue a query, it should end with error and leave exec_profile_handle uninitialised.
                    assert_cass_error_eq!(
                        cass_future_error_code(cass_session_execute(
                            session_raw.clone(),
                            statement_raw.clone().into_const()
                        )),
                        CassError::CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID
                    );
                    assert_eq!(
                        statement
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .inner()
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &nonexisting_name.to_owned().try_into().unwrap()
                    );
                    assert_cass_error_eq!(
                        cass_future_error_code(cass_session_execute_batch(
                            session_raw.clone(),
                            batch_raw.clone().into_const()
                        )),
                        CassError::CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID
                    );
                    assert_eq!(
                        batch
                            .exec_profile
                            .as_ref()
                            .unwrap()
                            .inner()
                            .read()
                            .unwrap()
                            .as_name()
                            .unwrap(),
                        &nonexisting_name.to_owned().try_into().unwrap()
                    );
                }
            }

            cass_future_wait_check_and_free(cass_session_close(session_raw.clone()));
            cass_execution_profile_free(profile_raw);
            cass_statement_free(statement_raw);
            cass_batch_free(batch_raw);
            cass_session_free(session_raw);
            cass_cluster_free(cluster_raw);
        }
        proxy
    }

    #[tokio::test]
    #[ntest::timeout(30000)]
    async fn retry_policy_on_statement_and_batch_is_handled_properly() {
        init_logger();
        test_with_one_proxy_one(
            retry_policy_on_statement_and_batch_is_handled_properly_do,
            retry_policy_on_statement_and_batch_is_handled_properly_rules(),
        )
        .with_current_subscriber()
        .await;
    }

    fn retry_policy_on_statement_and_batch_is_handled_properly_rules(
    ) -> impl IntoIterator<Item = RequestRule> {
        handshake_rules()
            .into_iter()
            .chain(iter::once(RequestRule(
                Condition::RequestOpcode(RequestOpcode::Query)
                    .or(Condition::RequestOpcode(RequestOpcode::Batch))
                    .and(Condition::BodyContainsCaseInsensitive(Box::new(
                        *b"SELECT host_id FROM system.",
                    )))
                    // this 1 differentiates Fallthrough and Default retry policies.
                    .and(Condition::TrueForLimitedTimes(1)),
                // We simulate the read timeout error in order to trigger DefaultRetryPolicy's
                // retry on the same node.
                // We don't use the example ReadTimeout error that is included in proxy,
                // because in order to trigger a retry we need data_present=false.
                RequestReaction::forge_with_error(DbError::ReadTimeout {
                    consistency: Consistency::All,
                    received: 1,
                    required: 1,
                    data_present: false,
                }),
            )))
            .chain(iter::once(RequestRule(
                Condition::RequestOpcode(RequestOpcode::Query)
                    .or(Condition::RequestOpcode(RequestOpcode::Batch))
                    .and(Condition::BodyContainsCaseInsensitive(Box::new(
                        *b"SELECT host_id FROM system.",
                    ))),
                // We make the second attempt return a hard, nonrecoverable error.
                RequestReaction::forge().read_failure(),
            )))
            .chain(generic_drop_queries_rules())
    }

    // This test aims to verify that the retry policy emulation works properly,
    // in any sequence of actions mutating the retry policy for a query.
    //
    // Below, the consecutive states of the test case are illustrated:
    //     Retry policy set on: ('F' - Fallthrough, 'D' - Default, '-' - no policy set)
    //     session default exec profile:   F F F F F F F F F F F F F F
    //     per stmt/batch exec profile:    - D - - D D D D D - - - D D
    //     stmt/batch (emulated):          - - - F F - F D F F - D D -
    fn retry_policy_on_statement_and_batch_is_handled_properly_do(
        node_addr: SocketAddr,
        mut proxy: RunningProxy,
    ) -> RunningProxy {
        unsafe {
            let cluster_raw = cass_cluster_new();
            let ip = node_addr.ip().to_string();
            let (c_ip, c_ip_len) = str_to_c_str_n(ip.as_str());

            assert_cass_error_eq!(
                cass_cluster_set_contact_points_n(cluster_raw.clone(), c_ip, c_ip_len,),
                CassError::CASS_OK
            );

            let fallthrough_policy = cass_retry_policy_fallthrough_new();
            let default_policy = cass_retry_policy_default_new();
            cass_cluster_set_retry_policy(cluster_raw.clone(), fallthrough_policy.clone());

            let session_raw = cass_session_new();

            let profile_raw = cass_execution_profile_new();
            // A name of a profile that will have been registered in the Cluster.
            let profile_name_c_str = make_c_str!("profile");

            assert_cass_error_eq!(
                cass_execution_profile_set_retry_policy(
                    profile_raw.clone(),
                    default_policy.clone()
                ),
                CassError::CASS_OK
            );

            let query = make_c_str!("SELECT host_id FROM system.local WHERE key='local'");
            let statement_raw = cass_statement_new(query, 0);
            let batch_raw = cass_batch_new(CassBatchType::CASS_BATCH_TYPE_LOGGED);
            assert_cass_error_eq!(
                cass_batch_add_statement(batch_raw.clone(), statement_raw.clone().into_const()),
                CassError::CASS_OK
            );

            assert_cass_error_eq!(
                cass_cluster_set_execution_profile(
                    cluster_raw.clone(),
                    profile_name_c_str,
                    profile_raw.clone().into_const(),
                ),
                CassError::CASS_OK
            );

            cass_future_wait_check_and_free(cass_session_connect(
                session_raw.clone(),
                cluster_raw.clone().into_const(),
            ));
            {
                let execute_query = || {
                    cass_future_error_code(cass_session_execute(
                        session_raw.clone(),
                        statement_raw.clone().into_const(),
                    ))
                };
                let execute_batch = || {
                    cass_future_error_code(cass_session_execute_batch(
                        session_raw.clone(),
                        batch_raw.clone().into_const(),
                    ))
                };

                fn reset_proxy_rules(proxy: &mut RunningProxy) {
                    proxy.running_nodes[0].change_request_rules(Some(
                        retry_policy_on_statement_and_batch_is_handled_properly_rules()
                            .into_iter()
                            .collect(),
                    ))
                }

                let assert_query_with_fallthrough_policy = |proxy: &mut RunningProxy| {
                    reset_proxy_rules(&mut *proxy);
                    assert_cass_error_eq!(
                        execute_query(),
                        CassError::CASS_ERROR_SERVER_READ_TIMEOUT,
                    );
                    reset_proxy_rules(&mut *proxy);
                    assert_cass_error_eq!(
                        execute_batch(),
                        CassError::CASS_ERROR_SERVER_READ_TIMEOUT,
                    );
                };

                let assert_query_with_default_policy = |proxy: &mut RunningProxy| {
                    reset_proxy_rules(&mut *proxy);
                    assert_cass_error_eq!(
                        execute_query(),
                        CassError::CASS_ERROR_SERVER_READ_FAILURE
                    );
                    reset_proxy_rules(&mut *proxy);
                    assert_cass_error_eq!(
                        execute_batch(),
                        CassError::CASS_ERROR_SERVER_READ_FAILURE
                    );
                };

                let set_provided_exec_profile = |name| {
                    // Set statement/batch exec profile.
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile(statement_raw.clone(), name,),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(batch_raw.clone(), name,),
                        CassError::CASS_OK
                    );
                };
                let set_exec_profile = || {
                    set_provided_exec_profile(profile_name_c_str);
                };
                let unset_exec_profile = || {
                    set_provided_exec_profile(std::ptr::null::<i8>());
                };
                let set_retry_policy_on_stmt = |policy: CassSharedPtr<CassRetryPolicy>| {
                    assert_cass_error_eq!(
                        cass_statement_set_retry_policy(statement_raw.clone(), policy.clone(),),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_retry_policy(batch_raw.clone(), policy,),
                        CassError::CASS_OK
                    );
                };
                let unset_retry_policy_on_stmt = || {
                    set_retry_policy_on_stmt(ArcFFI::null());
                };

                // ### START TESTING

                // With no exec profile nor retry policy set on statement/batch,
                // the default cluster-wide retry policy should be used: in this case, fallthrough.

                // F - -
                assert_query_with_fallthrough_policy(&mut proxy);

                // F D -
                set_exec_profile();
                assert_query_with_default_policy(&mut proxy);

                // F - -
                unset_exec_profile();
                assert_query_with_fallthrough_policy(&mut proxy);

                // F - F
                set_retry_policy_on_stmt(fallthrough_policy.clone());
                assert_query_with_fallthrough_policy(&mut proxy);

                // F D F
                set_exec_profile();
                assert_query_with_fallthrough_policy(&mut proxy);

                // F D -
                unset_retry_policy_on_stmt();
                assert_query_with_default_policy(&mut proxy);

                // F D F
                set_retry_policy_on_stmt(fallthrough_policy.clone());
                assert_query_with_fallthrough_policy(&mut proxy);

                // F D D
                set_retry_policy_on_stmt(default_policy.clone());
                assert_query_with_default_policy(&mut proxy);

                // F D F
                set_retry_policy_on_stmt(fallthrough_policy.clone());
                assert_query_with_fallthrough_policy(&mut proxy);

                // F - F
                unset_exec_profile();
                assert_query_with_fallthrough_policy(&mut proxy);

                // F - -
                unset_retry_policy_on_stmt();
                assert_query_with_fallthrough_policy(&mut proxy);

                // F - D
                set_retry_policy_on_stmt(default_policy);
                assert_query_with_default_policy(&mut proxy);

                // F D D
                set_exec_profile();
                assert_query_with_default_policy(&mut proxy);

                // F D -
                unset_retry_policy_on_stmt();
                assert_query_with_default_policy(&mut proxy);
            }

            cass_future_wait_check_and_free(cass_session_close(session_raw.clone()));
            cass_execution_profile_free(profile_raw);
            cass_statement_free(statement_raw);
            cass_batch_free(batch_raw);
            cass_session_free(session_raw);
            cass_cluster_free(cluster_raw);
        }
        proxy
    }

    #[test]
    #[ntest::timeout(5000)]
    fn session_with_latency_aware_load_balancing_does_not_panic() {
        unsafe {
            let cluster_raw = cass_cluster_new();

            // An IP with very little chance of having a Scylla node listening
            let ip = "127.0.1.231";
            let (c_ip, c_ip_len) = str_to_c_str_n(ip);

            assert_cass_error_eq!(
                cass_cluster_set_contact_points_n(cluster_raw.clone(), c_ip, c_ip_len),
                CassError::CASS_OK
            );
            cass_cluster_set_latency_aware_routing(cluster_raw.clone(), true as cass_bool_t);
            let session_raw = cass_session_new();
            let profile_raw = cass_execution_profile_new();
            assert_cass_error_eq!(
                cass_execution_profile_set_latency_aware_routing(
                    profile_raw.clone(),
                    true as cass_bool_t
                ),
                CassError::CASS_OK
            );
            let profile_name = make_c_str!("latency_aware");
            cass_cluster_set_execution_profile(
                cluster_raw.clone(),
                profile_name,
                profile_raw.clone().into_const(),
            );
            {
                let cass_future =
                    cass_session_connect(session_raw.clone(), cluster_raw.clone().into_const());
                cass_future_wait(cass_future);
                // The exact outcome is not important, we only test that we don't panic.
            }
            cass_execution_profile_free(profile_raw);
            cass_session_free(session_raw);
            cass_cluster_free(cluster_raw);
        }
    }

    rusty_fork_test! {
        #![rusty_fork(timeout_ms = 1000)]
        #[test]
        fn cluster_is_not_referenced_by_session_connect_future() {
            // An IP with very little chance of having a Scylla node listening
            let ip = "127.0.1.231";
            let (c_ip, c_ip_len) = str_to_c_str_n(ip);
            let profile_name = make_c_str!("latency_aware");

            unsafe {
                let cluster_raw = cass_cluster_new();

                assert_cass_error_eq!(
                    cass_cluster_set_contact_points_n(cluster_raw.clone(), c_ip, c_ip_len),
                    CassError::CASS_OK
                );
                cass_cluster_set_latency_aware_routing(cluster_raw.clone(), true as cass_bool_t);
                let session_raw = cass_session_new();
                let profile_raw = cass_execution_profile_new();
                assert_cass_error_eq!(
                    cass_execution_profile_set_latency_aware_routing(profile_raw.clone(), true as cass_bool_t),
                    CassError::CASS_OK
                );
                cass_cluster_set_execution_profile(cluster_raw.clone(), profile_name, profile_raw.clone().into_const());
                {
                    let cass_future = cass_session_connect(session_raw.clone(), cluster_raw.clone().into_const());

                    // This checks that we don't use-after-free the cluster inside the future.
                    cass_cluster_free(cluster_raw);

                    cass_future_wait(cass_future);
                    // The exact outcome is not important, we only test that we don't segfault.
                }
                cass_execution_profile_free(profile_raw);
                cass_session_free(session_raw);
            }
        }
    }
}
