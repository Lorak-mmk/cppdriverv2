use crate::argconv::*;
use crate::batch::CassBatch;
use crate::cass_error::*;
use crate::cass_types::{get_column_type, CassDataType, UDTDataType};
use crate::cluster::build_session_builder;
use crate::cluster::CassCluster;
use crate::exec_profile::{CassExecProfile, ExecProfileName, PerStatementExecProfile};
use crate::future::{CassFuture, CassFutureResult, CassResultValue};
use crate::metadata::create_table_metadata;
use crate::metadata::{CassKeyspaceMeta, CassMaterializedViewMeta, CassSchemaMeta};
use crate::query_result::Value::{CollectionValue, RegularValue};
use crate::query_result::{CassResult, CassResultData, CassRow, CassValue, Collection, Value};
use crate::statement::CassStatement;
use crate::statement::Statement;
use crate::types::{cass_uint64_t, size_t};
use crate::uuid::CassUuid;
use scylla::frame::response::result::{CqlValue, Row};
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
    ) -> *const CassFuture {
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
        let mut exec_profile_map = HashMap::with_capacity(exec_profile_builder_map.len());
        for (name, builder) in exec_profile_builder_map {
            exec_profile_map.insert(name, builder.build().await.into_handle());
        }

        let mut session_builder = session_builder_fut.await;
        if let Some(keyspace) = keyspace {
            session_builder = session_builder.use_keyspace(keyspace, false);
        }

        let session = session_builder
            .build()
            .await
            .map_err(|err| (CassError::from(&err), err.msg()))?;

        *session_guard = Some(CassSessionInner {
            session,
            exec_profile_map,
            client_id,
        });
        Ok(CassResultValue::Empty)
    }
}

pub type CassSession = RwLock<Option<CassSessionInner>>;

#[no_mangle]
pub unsafe extern "C" fn cass_session_new() -> *mut CassSession {
    let session = Arc::new(RwLock::new(None::<CassSessionInner>));
    Arc::into_raw(session) as *mut CassSession
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_connect(
    session_raw: *mut CassSession,
    cluster_raw: *const CassCluster,
) -> *const CassFuture {
    let session_opt = ptr_to_ref(session_raw);
    let cluster: &CassCluster = ptr_to_ref(cluster_raw);

    CassSessionInner::connect(session_opt, cluster, None)
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_connect_keyspace(
    session_raw: *mut CassSession,
    cluster_raw: *const CassCluster,
    keyspace: *const c_char,
) -> *const CassFuture {
    cass_session_connect_keyspace_n(session_raw, cluster_raw, keyspace, strlen(keyspace))
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_connect_keyspace_n(
    session_raw: *mut CassSession,
    cluster_raw: *const CassCluster,
    keyspace: *const c_char,
    keyspace_length: size_t,
) -> *const CassFuture {
    let session_opt = ptr_to_ref(session_raw);
    let cluster: &CassCluster = ptr_to_ref(cluster_raw);
    let keyspace = ptr_to_cstr_n(keyspace, keyspace_length).map(ToOwned::to_owned);

    CassSessionInner::connect(session_opt, cluster, keyspace)
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_execute_batch(
    session_raw: *mut CassSession,
    batch_raw: *const CassBatch,
) -> *const CassFuture {
    let session_opt = ptr_to_ref(session_raw);
    let batch_from_raw = ptr_to_ref(batch_raw);
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
                rows: None,
                metadata: Arc::new(CassResultData {
                    paging_state_response: PagingStateResponse::NoMorePages,
                    col_specs: vec![],
                    tracing_id: None,
                }),
            }))),
            Err(err) => Ok(CassResultValue::QueryError(Arc::new(err))),
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
            QueryError::TimeoutError,
        ))),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_execute(
    session_raw: *mut CassSession,
    statement_raw: *const CassStatement,
) -> *const CassFuture {
    let session_opt = clone_arced(session_raw);

    // DO NOT refer to `statement_opt` inside the async block, as I've done just to face a segfault.
    let statement_opt = ptr_to_ref(statement_raw);
    let paging_state = statement_opt.paging_state.clone();
    let paging_enabled = statement_opt.paging_enabled;
    let bound_values = statement_opt.bound_values.clone();
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
            Statement::Simple(query) => query.query.set_execution_profile_handle(handle),
            Statement::Prepared(prepared) => {
                Arc::make_mut(prepared).set_execution_profile_handle(handle)
            }
        }

        let query_res: Result<(QueryResult, PagingStateResponse), QueryError> = match statement {
            Statement::Simple(query) => {
                if paging_enabled {
                    session
                        .query_single_page(query.query, bound_values, paging_state)
                        .await
                } else {
                    session
                        .query_unpaged(query.query, bound_values)
                        .await
                        .map(|result| (result, PagingStateResponse::NoMorePages))
                }
            }
            Statement::Prepared(prepared) => {
                if paging_enabled {
                    session
                        .execute_single_page(&prepared, bound_values, paging_state)
                        .await
                } else {
                    session
                        .execute_unpaged(&prepared, bound_values)
                        .await
                        .map(|result| (result, PagingStateResponse::NoMorePages))
                }
            }
        };

        match query_res {
            Ok((result, paging_state_response)) => {
                let metadata = Arc::new(CassResultData {
                    paging_state_response,
                    col_specs: result.col_specs().to_vec(),
                    tracing_id: result.tracing_id,
                });
                let cass_rows = create_cass_rows_from_rows(result.rows, &metadata);
                let cass_result = Arc::new(CassResult {
                    rows: cass_rows,
                    metadata,
                });

                Ok(CassResultValue::QueryResult(cass_result))
            }
            Err(err) => Ok(CassResultValue::QueryError(Arc::new(err))),
        }
    };

    match request_timeout_ms {
        Some(timeout_ms) => {
            CassFuture::make_raw(async move { request_with_timeout(timeout_ms, future).await })
        }
        None => CassFuture::make_raw(future),
    }
}

fn create_cass_rows_from_rows(
    rows: Option<Vec<Row>>,
    metadata: &Arc<CassResultData>,
) -> Option<Vec<CassRow>> {
    let rows = rows?;
    let cass_rows = rows
        .into_iter()
        .map(|r| CassRow {
            columns: create_cass_row_columns(r, metadata),
            result_metadata: metadata.clone(),
        })
        .collect();

    Some(cass_rows)
}

fn create_cass_row_columns(row: Row, metadata: &Arc<CassResultData>) -> Vec<CassValue> {
    row.columns
        .into_iter()
        .zip(metadata.col_specs.iter())
        .map(|(val, col)| {
            let column_type = Arc::new(get_column_type(&col.typ));
            CassValue {
                value: val.map(|col_val| get_column_value(col_val, &column_type)),
                value_type: column_type,
            }
        })
        .collect()
}

fn get_column_value(column: CqlValue, column_type: &Arc<CassDataType>) -> Value {
    match (column, column_type.as_ref()) {
        (
            CqlValue::List(list),
            CassDataType::List {
                typ: Some(list_type),
                ..
            },
        ) => CollectionValue(Collection::List(
            list.into_iter()
                .map(|val| CassValue {
                    value_type: list_type.clone(),
                    value: Some(get_column_value(val, list_type)),
                })
                .collect(),
        )),
        (
            CqlValue::Map(map),
            CassDataType::Map {
                key_type: Some(key_typ),
                val_type: Some(value_type),
                ..
            },
        ) => CollectionValue(Collection::Map(
            map.into_iter()
                .map(|(key, val)| {
                    (
                        CassValue {
                            value_type: key_typ.clone(),
                            value: Some(get_column_value(key, key_typ)),
                        },
                        CassValue {
                            value_type: value_type.clone(),
                            value: Some(get_column_value(val, value_type)),
                        },
                    )
                })
                .collect(),
        )),
        (
            CqlValue::Set(set),
            CassDataType::Set {
                typ: Some(set_type),
                ..
            },
        ) => CollectionValue(Collection::Set(
            set.into_iter()
                .map(|val| CassValue {
                    value_type: set_type.clone(),
                    value: Some(get_column_value(val, set_type)),
                })
                .collect(),
        )),
        (
            CqlValue::UserDefinedType {
                keyspace,
                type_name,
                fields,
            },
            CassDataType::UDT(udt_type),
        ) => CollectionValue(Collection::UserDefinedType {
            keyspace,
            type_name,
            fields: fields
                .into_iter()
                .enumerate()
                .map(|(index, (name, val_opt))| {
                    let udt_field_type_opt = udt_type.get_field_by_index(index);
                    if let (Some(val), Some(udt_field_type)) = (val_opt, udt_field_type_opt) {
                        return (
                            name,
                            Some(CassValue {
                                value_type: udt_field_type.clone(),
                                value: Some(get_column_value(val, udt_field_type)),
                            }),
                        );
                    }
                    (name, None)
                })
                .collect(),
        }),
        (CqlValue::Tuple(tuple), CassDataType::Tuple(tuple_types)) => {
            CollectionValue(Collection::Tuple(
                tuple
                    .into_iter()
                    .enumerate()
                    .map(|(index, val_opt)| {
                        val_opt
                            .zip(tuple_types.get(index))
                            .map(|(val, tuple_field_type)| CassValue {
                                value_type: tuple_field_type.clone(),
                                value: Some(get_column_value(val, tuple_field_type)),
                            })
                    })
                    .collect(),
            ))
        }
        (regular_value, _) => RegularValue(regular_value),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare_from_existing(
    cass_session: *mut CassSession,
    statement: *const CassStatement,
) -> *const CassFuture {
    let session = ptr_to_ref(cass_session);
    let cass_statement = ptr_to_ref(statement);
    let statement = cass_statement.statement.clone();

    CassFuture::make_raw(async move {
        let query = match &statement {
            Statement::Simple(q) => q,
            Statement::Prepared(ps) => {
                return Ok(CassResultValue::Prepared(ps.clone()));
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
            .map_err(|err| (CassError::from(&err), err.msg()))?;

        Ok(CassResultValue::Prepared(Arc::new(prepared)))
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare(
    session: *mut CassSession,
    query: *const c_char,
) -> *const CassFuture {
    cass_session_prepare_n(session, query, strlen(query))
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_prepare_n(
    cass_session_raw: *mut CassSession,
    query: *const c_char,
    query_length: size_t,
) -> *const CassFuture {
    let query_str = match ptr_to_cstr_n(query, query_length) {
        Some(v) => v,
        None => return std::ptr::null(),
    };
    let query = Query::new(query_str.to_string());
    let cass_session: &CassSession = ptr_to_ref(cass_session_raw);

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
            .map_err(|err| (CassError::from(&err), err.msg()))?;

        // Set Cpp Driver default configuration for queries:
        prepared.set_consistency(Consistency::One);

        Ok(CassResultValue::Prepared(Arc::new(prepared)))
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_free(session_raw: *mut CassSession) {
    free_arced(session_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_close(session: *mut CassSession) -> *const CassFuture {
    let session_opt = ptr_to_ref(session);

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
pub unsafe extern "C" fn cass_session_get_client_id(session: *const CassSession) -> CassUuid {
    let cass_session = ptr_to_ref(session);

    let client_id: uuid::Uuid = cass_session.blocking_read().as_ref().unwrap().client_id;
    client_id.into()
}

#[no_mangle]
pub unsafe extern "C" fn cass_session_get_schema_meta(
    session: *const CassSession,
) -> *const CassSchemaMeta {
    let cass_session = ptr_to_ref(session);
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
                Arc::new(CassDataType::UDT(UDTDataType::create_with_params(
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

    Box::into_raw(Box::new(CassSchemaMeta { keyspaces }))
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
        argconv::{make_c_str, ptr_to_ref},
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
        retry_policy::{cass_retry_policy_default_new, cass_retry_policy_fallthrough_new},
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

    unsafe fn cass_future_wait_check_and_free(fut: *const CassFuture) {
        cass_future_wait(fut);
        if cass_future_error_code(fut) != CassError::CASS_OK {
            let mut message: *const c_char = std::ptr::null();
            let mut message_len: size_t = 0;
            cass_future_error_message(fut as *mut CassFuture, &mut message, &mut message_len);
            eprintln!("{:?}", ptr_to_cstr_n(message, message_len));
        }
        assert_cass_error_eq!(cass_future_error_code(fut), CassError::CASS_OK);
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
                cass_cluster_set_contact_points_n(cluster_raw, c_ip, c_ip_len),
                CassError::CASS_OK
            );
            let session_raw = cass_session_new();
            let profile_raw = cass_execution_profile_new();
            {
                cass_future_wait_check_and_free(cass_session_connect(session_raw, cluster_raw));
                // Initially, the profile map is empty.

                assert!(ptr_to_ref(session_raw)
                    .blocking_read()
                    .as_ref()
                    .unwrap()
                    .exec_profile_map
                    .is_empty());

                cass_cluster_set_execution_profile(cluster_raw, make_c_str!("prof"), profile_raw);
                // Mutations in cluster do not affect the session that was connected before.
                assert!(ptr_to_ref(session_raw)
                    .blocking_read()
                    .as_ref()
                    .unwrap()
                    .exec_profile_map
                    .is_empty());

                cass_future_wait_check_and_free(cass_session_close(session_raw));

                // Mutations in cluster are now propagated to the session.
                cass_future_wait_check_and_free(cass_session_connect(session_raw, cluster_raw));
                let profile_map_keys = ptr_to_ref(session_raw)
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
                cass_future_wait_check_and_free(cass_session_close(session_raw));
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
                cass_cluster_set_contact_points_n(cluster_raw, c_ip, c_ip_len),
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
                cass_batch_add_statement(batch_raw, statement_raw),
                CassError::CASS_OK
            );

            assert_cass_error_eq!(
                cass_cluster_set_execution_profile(cluster_raw, valid_name_c_str, profile_raw,),
                CassError::CASS_OK
            );

            cass_future_wait_check_and_free(cass_session_connect(session_raw, cluster_raw));
            {
                /* Test valid configurations */
                let statement = ptr_to_ref(statement_raw);
                let batch = ptr_to_ref(batch_raw);
                {
                    assert!(statement.exec_profile.is_none());
                    assert!(batch.exec_profile.is_none());

                    // Set exec profile - it is not yet resolved.
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile(statement_raw, valid_name_c_str,),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(batch_raw, valid_name_c_str,),
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
                        cass_future_error_code(cass_session_execute(session_raw, statement_raw)),
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
                        cass_future_error_code(cass_session_execute_batch(session_raw, batch_raw,)),
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
                        cass_statement_set_execution_profile(statement_raw, std::ptr::null::<i8>()),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(batch_raw, std::ptr::null::<i8>()),
                        CassError::CASS_OK
                    );
                    assert!(statement.exec_profile.is_none());
                    assert!(batch.exec_profile.is_none());

                    // valid name again, but of nonexisting profile!
                    assert_cass_error_eq!(
                        cass_statement_set_execution_profile_n(
                            statement_raw,
                            nonexisting_name_c_str,
                            nonexisting_name_len,
                        ),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile_n(
                            batch_raw,
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
                        cass_future_error_code(cass_session_execute(session_raw, statement_raw)),
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
                        cass_future_error_code(cass_session_execute_batch(session_raw, batch_raw)),
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

            cass_future_wait_check_and_free(cass_session_close(session_raw));
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
                cass_cluster_set_contact_points_n(cluster_raw, c_ip, c_ip_len,),
                CassError::CASS_OK
            );

            let fallthrough_policy = cass_retry_policy_fallthrough_new();
            let default_policy = cass_retry_policy_default_new();
            cass_cluster_set_retry_policy(cluster_raw, fallthrough_policy);

            let session_raw = cass_session_new();

            let profile_raw = cass_execution_profile_new();
            // A name of a profile that will have been registered in the Cluster.
            let profile_name_c_str = make_c_str!("profile");

            assert_cass_error_eq!(
                cass_execution_profile_set_retry_policy(profile_raw, default_policy),
                CassError::CASS_OK
            );

            let query = make_c_str!("SELECT host_id FROM system.local");
            let statement_raw = cass_statement_new(query, 0);
            let batch_raw = cass_batch_new(CassBatchType::CASS_BATCH_TYPE_LOGGED);
            assert_cass_error_eq!(
                cass_batch_add_statement(batch_raw, statement_raw),
                CassError::CASS_OK
            );

            assert_cass_error_eq!(
                cass_cluster_set_execution_profile(cluster_raw, profile_name_c_str, profile_raw,),
                CassError::CASS_OK
            );

            cass_future_wait_check_and_free(cass_session_connect(session_raw, cluster_raw));
            {
                let execute_query =
                    || cass_future_error_code(cass_session_execute(session_raw, statement_raw));
                let execute_batch =
                    || cass_future_error_code(cass_session_execute_batch(session_raw, batch_raw));

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
                        cass_statement_set_execution_profile(statement_raw, name,),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_execution_profile(batch_raw, name,),
                        CassError::CASS_OK
                    );
                };
                let set_exec_profile = || {
                    set_provided_exec_profile(profile_name_c_str);
                };
                let unset_exec_profile = || {
                    set_provided_exec_profile(std::ptr::null::<i8>());
                };
                let set_retry_policy_on_stmt = |policy| {
                    assert_cass_error_eq!(
                        cass_statement_set_retry_policy(statement_raw, policy,),
                        CassError::CASS_OK
                    );
                    assert_cass_error_eq!(
                        cass_batch_set_retry_policy(batch_raw, policy,),
                        CassError::CASS_OK
                    );
                };
                let unset_retry_policy_on_stmt = || {
                    set_retry_policy_on_stmt(std::ptr::null());
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
                set_retry_policy_on_stmt(fallthrough_policy);
                assert_query_with_fallthrough_policy(&mut proxy);

                // F D F
                set_exec_profile();
                assert_query_with_fallthrough_policy(&mut proxy);

                // F D -
                unset_retry_policy_on_stmt();
                assert_query_with_default_policy(&mut proxy);

                // F D F
                set_retry_policy_on_stmt(fallthrough_policy);
                assert_query_with_fallthrough_policy(&mut proxy);

                // F D D
                set_retry_policy_on_stmt(default_policy);
                assert_query_with_default_policy(&mut proxy);

                // F D F
                set_retry_policy_on_stmt(fallthrough_policy);
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

            cass_future_wait_check_and_free(cass_session_close(session_raw));
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
                cass_cluster_set_contact_points_n(cluster_raw, c_ip, c_ip_len),
                CassError::CASS_OK
            );
            cass_cluster_set_latency_aware_routing(cluster_raw, true as cass_bool_t);
            let session_raw = cass_session_new();
            let profile_raw = cass_execution_profile_new();
            assert_cass_error_eq!(
                cass_execution_profile_set_latency_aware_routing(profile_raw, true as cass_bool_t),
                CassError::CASS_OK
            );
            let profile_name = make_c_str!("latency_aware");
            cass_cluster_set_execution_profile(cluster_raw, profile_name, profile_raw);
            {
                let cass_future = cass_session_connect(session_raw, cluster_raw);
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
                    cass_cluster_set_contact_points_n(cluster_raw, c_ip, c_ip_len),
                    CassError::CASS_OK
                );
                cass_cluster_set_latency_aware_routing(cluster_raw, true as cass_bool_t);
                let session_raw = cass_session_new();
                let profile_raw = cass_execution_profile_new();
                assert_cass_error_eq!(
                    cass_execution_profile_set_latency_aware_routing(profile_raw, true as cass_bool_t),
                    CassError::CASS_OK
                );
                cass_cluster_set_execution_profile(cluster_raw, profile_name, profile_raw);
                {
                    let cass_future = cass_session_connect(session_raw, cluster_raw);

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
