use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_error::CassErrorMessage;
use crate::cass_error::ToCassError;
use crate::prepared::CassPrepared;
use crate::query_error::CassErrorResult;
use crate::query_result::CassResult;
use crate::types::*;
use crate::uuid::CassUuid;
use crate::RUNTIME;
use futures::future;
use std::future::Future;
use std::mem;
use std::os::raw::c_void;
use std::sync::{Arc, Condvar, Mutex};
use tokio::task::JoinHandle;
use tokio::time::Duration;

pub enum CassResultValue {
    Empty,
    QueryResult(Arc<CassResult>),
    QueryError(Arc<CassErrorResult>),
    Prepared(Arc<CassPrepared>),
}

type CassFutureError = (CassError, String);

pub type CassFutureResult = Result<CassResultValue, CassFutureError>;

pub type CassFutureCallback =
    Option<unsafe extern "C" fn(future: *const CassFuture, data: *mut c_void)>;

struct BoundCallback {
    pub cb: CassFutureCallback,
    pub data: *mut c_void,
}

// *mut c_void is not Send, so Rust will have to take our word
// that we won't screw something up
unsafe impl Send for BoundCallback {}

impl BoundCallback {
    fn invoke(self, fut_ptr: *const CassFuture) {
        unsafe {
            self.cb.unwrap()(fut_ptr, self.data);
        }
    }
}

#[derive(Default)]
struct CassFutureState {
    value: Option<CassFutureResult>,
    err_string: Option<String>,
    callback: Option<BoundCallback>,
    join_handle: Option<JoinHandle<()>>,
}

pub struct CassFuture {
    state: Mutex<CassFutureState>,
    wait_for_value: Condvar,
}

impl FFI for CassFuture {
    type Ownership = OwnershipShared;
}

/// An error that can appear during `cass_future_wait_timed`.
enum FutureError {
    TimeoutError,
    InvalidDuration,
}

/// The timeout appeared when we tried to await `JoinHandle`.
/// This errors contains the original handle, so it can be awaited later again.
struct JoinHandleTimeout(JoinHandle<()>);

impl CassFuture {
    pub fn make_raw(
        fut: impl Future<Output = CassFutureResult> + Send + 'static,
    ) -> CassSharedPtr<CassFuture> {
        Self::new_from_future(fut).into_raw()
    }

    pub fn new_from_future(
        fut: impl Future<Output = CassFutureResult> + Send + 'static,
    ) -> Arc<CassFuture> {
        let cass_fut = Arc::new(CassFuture {
            state: Mutex::new(Default::default()),
            wait_for_value: Condvar::new(),
        });
        let cass_fut_clone = cass_fut.clone();
        let join_handle = RUNTIME.spawn(async move {
            let r = fut.await;
            let maybe_cb = {
                let mut guard = cass_fut_clone.state.lock().unwrap();
                guard.value = Some(r);
                // Take the callback and call it after releasing the lock
                guard.callback.take()
            };
            if let Some(bound_cb) = maybe_cb {
                let fut_ptr = ArcFFI::as_ptr(&cass_fut_clone);
                bound_cb.invoke(ArcFFI::to_raw(&fut_ptr));
            }

            cass_fut_clone.wait_for_value.notify_all();
        });
        {
            let mut lock = cass_fut.state.lock().unwrap();
            lock.join_handle = Some(join_handle);
        }
        cass_fut
    }

    pub fn new_ready(r: CassFutureResult) -> Arc<Self> {
        Arc::new(CassFuture {
            state: Mutex::new(CassFutureState {
                value: Some(r),
                ..Default::default()
            }),
            wait_for_value: Condvar::new(),
        })
    }

    pub fn with_waited_result<T>(&self, f: impl FnOnce(&mut CassFutureResult) -> T) -> T {
        self.with_waited_state(|s| f(s.value.as_mut().unwrap()))
    }

    /// Awaits the future until completion.
    ///
    /// There are two possible cases:
    /// - noone is currently working on the future -> we take the ownership
    ///   of JoinHandle (future) and we poll it until completion.
    /// - some other thread is working on the future -> we wait on the condition
    ///   variable to get an access to the future's state. Once we are notified,
    ///   there are two cases:
    ///     - JoinHandle is consumed -> some other thread already resolved the future.
    ///       We can return.
    ///     - JoinHandle is Some -> some other thread was working on the future, but
    ///       timed out (see [CassFuture::with_waited_state_timed]). We need to
    ///       take the ownership of the handle, and complete the work.
    fn with_waited_state<T>(&self, f: impl FnOnce(&mut CassFutureState) -> T) -> T {
        let mut guard = self.state.lock().unwrap();
        loop {
            let handle = guard.join_handle.take();
            if let Some(handle) = handle {
                mem::drop(guard);
                // unwrap: JoinError appears only when future either panic'ed or canceled.
                RUNTIME.block_on(handle).unwrap();
                guard = self.state.lock().unwrap();
            } else {
                guard = self
                    .wait_for_value
                    .wait_while(guard, |state| {
                        state.value.is_none() && state.join_handle.is_none()
                    })
                    // unwrap: Error appears only when mutex is poisoned.
                    .unwrap();
                if guard.join_handle.is_some() {
                    // join_handle was none, and now it isn't - some other thread must
                    // have timed out and returned the handle. We need to take over
                    // the work of completing the future. To do that, we go into
                    // another iteration so that we land in the branch with block_on.
                    continue;
                }
            }
            return f(&mut guard);
        }
    }

    fn with_waited_result_timed<T>(
        &self,
        f: impl FnOnce(&mut CassFutureResult) -> T,
        timeout_duration: Duration,
    ) -> Result<T, FutureError> {
        self.with_waited_state_timed(|s| f(s.value.as_mut().unwrap()), timeout_duration)
    }

    /// Tries to await the future with a given timeout.
    ///
    /// There are two possible cases:
    /// - noone is currently working on the future -> we take the ownership
    ///   of JoinHandle (future) and we try to poll it with given timeout.
    ///   If we timed out, we need to return the unfinished JoinHandle, so
    ///   some other thread can complete the future later.
    /// - some other thread is working on the future -> we wait on the condition
    ///   variable to get an access to the future's state.
    ///   Once we are notified (before the timeout), there are two cases.
    ///     - JoinHandle is consumed -> some other thread already resolved the future.
    ///       We can return.
    ///     - JoinHandle is Some -> some other thread was working on the future, but
    ///       timed out (see [CassFuture::with_waited_state_timed]). We need to
    ///       take the ownership of the handle, and continue the work.
    fn with_waited_state_timed<T>(
        &self,
        f: impl FnOnce(&mut CassFutureState) -> T,
        timeout_duration: Duration,
    ) -> Result<T, FutureError> {
        let mut guard = self.state.lock().unwrap();
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout_duration)
            .ok_or(FutureError::InvalidDuration)?;

        loop {
            let handle = guard.join_handle.take();
            if let Some(handle) = handle {
                mem::drop(guard);
                // Need to wrap it with async{} block, so the timeout is lazily executed inside the runtime.
                // See mention about panics: https://docs.rs/tokio/latest/tokio/time/fn.timeout.html.
                let timed = async {
                    let sleep_future = tokio::time::sleep_until(deadline);
                    tokio::pin!(sleep_future);
                    let value = future::select(handle, sleep_future).await;
                    match value {
                        future::Either::Left((result, _)) => Ok(result),
                        future::Either::Right((_, handle)) => Err(JoinHandleTimeout(handle)),
                    }
                };
                match RUNTIME.block_on(timed) {
                    Err(JoinHandleTimeout(returned_handle)) => {
                        // We timed out. so we can't finish waiting for the future.
                        // The problem is that if current thread executor is used,
                        // then no one will run this future - other threads will
                        // go into the branch with condvar and wait there.
                        // To fix that:
                        //  - Return the join handle, so that next thread can take it
                        //  - Signal one thread, so that if all other consumers are
                        //    already waiting on condvar, one of them wakes up and
                        //    picks up the work.
                        guard = self.state.lock().unwrap();
                        guard.join_handle = Some(returned_handle);
                        self.wait_for_value.notify_one();
                        return Err(FutureError::TimeoutError);
                    }
                    // unwrap: JoinError appears only when future either panic'ed or canceled.
                    Ok(result) => result.unwrap(),
                };
                guard = self.state.lock().unwrap();
            } else {
                let remaining_timeout = deadline.duration_since(tokio::time::Instant::now());
                let (guard_result, timeout_result) = self
                    .wait_for_value
                    .wait_timeout_while(guard, remaining_timeout, |state| {
                        state.value.is_none() && state.join_handle.is_none()
                    })
                    // unwrap: Error appears only when mutex is poisoned.
                    .unwrap();
                if timeout_result.timed_out() {
                    return Err(FutureError::TimeoutError);
                }
                guard = guard_result;
                if guard.join_handle.is_some() {
                    // join_handle was none, and now it isn't - some other thread must
                    // have timed out and returned the handle. We need to take over
                    // the work of completing the future. To do that, we go into
                    // another iteration so that we land in the branch with block_on.
                    continue;
                }
            }

            return Ok(f(&mut guard));
        }
    }

    pub fn set_callback(
        &self,
        self_ptr: *const CassFuture,
        cb: CassFutureCallback,
        data: *mut c_void,
    ) -> CassError {
        let mut lock = self.state.lock().unwrap();
        if lock.callback.is_some() {
            // Another callback has been already set
            return CassError::CASS_ERROR_LIB_CALLBACK_ALREADY_SET;
        }
        let bound_cb = BoundCallback { cb, data };
        if lock.value.is_some() {
            // The value is already available, we need to call the callback ourselves
            mem::drop(lock);
            bound_cb.invoke(self_ptr);
            return CassError::CASS_OK;
        }
        // Store the callback
        lock.callback = Some(bound_cb);
        CassError::CASS_OK
    }

    fn into_raw(self: Arc<Self>) -> CassSharedPtr<Self> {
        ArcFFI::into_ptr(self)
    }
}

// Do not remove; this asserts that `CassFuture` implements Send + Sync,
// which is required by the cpp-driver (saying that `CassFuture` is thread-safe).
#[allow(unused)]
trait CheckSendSync: Send + Sync {}
impl CheckSendSync for CassFuture {}

#[no_mangle]
pub unsafe extern "C" fn cass_future_set_callback(
    future_raw: CassSharedPtr<CassFuture>,
    callback: CassFutureCallback,
    data: *mut ::std::os::raw::c_void,
) -> CassError {
    ArcFFI::as_ref(&future_raw)
        .unwrap()
        .set_callback(ArcFFI::to_raw(&future_raw), callback, data)
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_wait(future_raw: CassSharedPtr<CassFuture>) {
    ArcFFI::as_ref(&future_raw)
        .unwrap()
        .with_waited_result(|_| ());
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_wait_timed(
    future_raw: CassSharedPtr<CassFuture>,
    timeout_us: cass_duration_t,
) -> cass_bool_t {
    ArcFFI::as_ref(&future_raw)
        .unwrap()
        .with_waited_result_timed(|_| (), Duration::from_micros(timeout_us))
        .is_ok() as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_ready(future_raw: CassSharedPtr<CassFuture>) -> cass_bool_t {
    let state_guard = ArcFFI::as_ref(&future_raw).unwrap().state.lock().unwrap();
    match state_guard.value {
        None => cass_false,
        Some(_) => cass_true,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_error_code(
    future_raw: CassSharedPtr<CassFuture>,
) -> CassError {
    ArcFFI::as_ref(&future_raw)
        .unwrap()
        .with_waited_result(|r: &mut CassFutureResult| match r {
            Ok(CassResultValue::QueryError(err)) => err.to_cass_error(),
            Err((err, _)) => *err,
            _ => CassError::CASS_OK,
        })
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_error_message(
    future: CassSharedPtr<CassFuture>,
    message: *mut *const ::std::os::raw::c_char,
    message_length: *mut size_t,
) {
    ArcFFI::as_ref(&future)
        .unwrap()
        .with_waited_state(|state: &mut CassFutureState| {
            let value = &state.value;
            let msg = state
                .err_string
                .get_or_insert_with(|| match value.as_ref().unwrap() {
                    Ok(CassResultValue::QueryError(err)) => err.msg(),
                    Err((_, s)) => s.msg(),
                    _ => "".to_string(),
                });
            write_str_to_c(msg.as_str(), message, message_length);
        });
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_free(future_raw: CassSharedPtr<CassFuture>) {
    ArcFFI::free(future_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_get_result(
    future_raw: CassSharedPtr<CassFuture>,
) -> CassSharedPtr<CassResult> {
    ArcFFI::as_ref(&future_raw)
        .unwrap()
        .with_waited_result(|r: &mut CassFutureResult| -> Option<Arc<CassResult>> {
            match r.as_ref().ok()? {
                CassResultValue::QueryResult(qr) => Some(qr.clone()),
                _ => None,
            }
        })
        .map_or(ArcFFI::null(), ArcFFI::into_ptr)
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_get_error_result(
    future_raw: CassSharedPtr<CassFuture>,
) -> CassSharedPtr<CassErrorResult> {
    ArcFFI::as_ref(&future_raw)
        .unwrap()
        .with_waited_result(|r: &mut CassFutureResult| -> Option<Arc<CassErrorResult>> {
            match r.as_ref().ok()? {
                CassResultValue::QueryError(qr) => Some(qr.clone()),
                _ => None,
            }
        })
        .map_or(ArcFFI::null(), ArcFFI::into_ptr)
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_get_prepared(
    future_raw: CassSharedPtr<CassFuture>,
) -> CassSharedPtr<CassPrepared> {
    ArcFFI::as_ref(&future_raw)
        .unwrap()
        .with_waited_result(|r: &mut CassFutureResult| -> Option<Arc<CassPrepared>> {
            match r.as_ref().ok()? {
                CassResultValue::Prepared(p) => Some(p.clone()),
                _ => None,
            }
        })
        .map_or(ArcFFI::null(), ArcFFI::into_ptr)
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_tracing_id(
    future: CassSharedPtr<CassFuture>,
    tracing_id: *mut CassUuid,
) -> CassError {
    ArcFFI::as_ref(&future)
        .unwrap()
        .with_waited_result(|r: &mut CassFutureResult| match r {
            Ok(CassResultValue::QueryResult(result)) => match result.tracing_id {
                Some(id) => {
                    *tracing_id = CassUuid::from(id);
                    CassError::CASS_OK
                }
                None => CassError::CASS_ERROR_LIB_NO_TRACING_ID,
            },
            _ => CassError::CASS_ERROR_LIB_INVALID_FUTURE_TYPE,
        })
}

#[cfg(test)]
mod tests {
    use crate::testing::{assert_cass_error_eq, assert_cass_future_error_message_eq};

    use super::*;
    use std::{
        os::raw::c_char,
        thread::{self},
        time::Duration,
    };

    // This is not a particularly smart test, but if some thread is granted access the value
    // before it is truly computed, then weird things should happen, even a segfault.
    // In the incorrect implementation that inspired this test to be written, this test
    // results with unwrap on a PoisonError on the CassFuture's mutex.
    #[test]
    #[ntest::timeout(100)]
    fn cass_future_thread_safety() {
        const ERROR_MSG: &str = "NOBODY EXPECTED SPANISH INQUISITION";
        let fut = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err((CassError::CASS_OK, ERROR_MSG.into()))
        };
        let cass_fut = CassFuture::make_raw(fut);

        struct PtrWrapper(CassSharedPtr<CassFuture>);
        unsafe impl Send for PtrWrapper {}
        let wrapped_cass_fut = PtrWrapper(unsafe { cass_fut.clone() });
        unsafe {
            let handle = thread::spawn(move || {
                let wrapper = wrapped_cass_fut;
                let PtrWrapper(cass_fut) = wrapper;
                assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));
            });

            assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));

            handle.join().unwrap();
            cass_future_free(cass_fut);
        }
    }

    // This test makes sure that the future resolves even if timeout happens.
    #[test]
    #[ntest::timeout(200)]
    fn cass_future_resolves_after_timeout() {
        const ERROR_MSG: &str = "NOBODY EXPECTED SPANISH INQUISITION";
        const HUNDRED_MILLIS_IN_MICROS: u64 = 100 * 1000;
        let fut = async move {
            tokio::time::sleep(Duration::from_micros(HUNDRED_MILLIS_IN_MICROS)).await;
            Err((CassError::CASS_OK, ERROR_MSG.into()))
        };
        let cass_fut = CassFuture::make_raw(fut);

        unsafe {
            // This should timeout on tokio::time::timeout.
            let timed_result =
                cass_future_wait_timed(cass_fut.clone(), HUNDRED_MILLIS_IN_MICROS / 5);
            assert_eq!(0, timed_result);

            // This should timeout as well.
            let timed_result =
                cass_future_wait_timed(cass_fut.clone(), HUNDRED_MILLIS_IN_MICROS / 5);
            assert_eq!(0, timed_result);

            // Verify that future eventually resolves, even though timeouts occurred before.
            assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));

            cass_future_free(cass_fut);
        }
    }

    // This test checks whether the future callback is executed correctly when:
    // - a future is awaited indefinitely
    // - a future is awaited, after the timeout appeared (_wait_timed)
    // - a future is not awaited. We simply sleep, and let the tokio runtime resolve
    //   the future, and execute its callback
    #[test]
    #[ntest::timeout(600)]
    #[allow(clippy::disallowed_methods)]
    fn test_cass_future_callback() {
        unsafe {
            const ERROR_MSG: &str = "NOBODY EXPECTED SPANISH INQUISITION";
            const HUNDRED_MILLIS_IN_MICROS: u64 = 100 * 1000;

            let create_future_and_flag = || {
                unsafe extern "C" fn mark_flag_cb(_fut: *const CassFuture, data: *mut c_void) {
                    let flag = data as *mut bool;
                    *flag = true;
                }

                let fut = async move {
                    tokio::time::sleep(Duration::from_micros(HUNDRED_MILLIS_IN_MICROS)).await;
                    Err((CassError::CASS_OK, ERROR_MSG.into()))
                };
                let cass_fut = CassFuture::make_raw(fut);
                let flag = Box::new(false);
                let flag_ptr = Box::into_raw(flag);

                assert_cass_error_eq!(
                    cass_future_set_callback(
                        cass_fut.clone(),
                        Some(mark_flag_cb),
                        flag_ptr as *mut c_void
                    ),
                    CassError::CASS_OK
                );

                (cass_fut, flag_ptr)
            };

            // Callback executed after awaiting.
            {
                let (cass_fut, flag_ptr) = create_future_and_flag();
                cass_future_wait(cass_fut.clone());

                assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));
                assert!(*flag_ptr);

                cass_future_free(cass_fut);
                let _ = Box::from_raw(flag_ptr);
            }

            // Future awaited via `assert_cass_future_error_message_eq`.
            {
                let (cass_fut, flag_ptr) = create_future_and_flag();

                assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));
                assert!(*flag_ptr);

                cass_future_free(cass_fut);
                let _ = Box::from_raw(flag_ptr);
            }

            // Callback executed after timeouts.
            {
                let (cass_fut, flag_ptr) = create_future_and_flag();

                // This should timeout on tokio::time::timeout.
                let timed_result =
                    cass_future_wait_timed(cass_fut.clone(), HUNDRED_MILLIS_IN_MICROS / 5);
                assert_eq!(0, timed_result);
                // This should timeout as well.
                let timed_result =
                    cass_future_wait_timed(cass_fut.clone(), HUNDRED_MILLIS_IN_MICROS / 5);
                assert_eq!(0, timed_result);

                // Await and check result.
                assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));
                assert!(*flag_ptr);

                cass_future_free(cass_fut);
                let _ = Box::from_raw(flag_ptr);
            }

            // Don't await the future. Just sleep.
            {
                let (cass_fut, flag_ptr) = create_future_and_flag();

                RUNTIME.block_on(async {
                    tokio::time::sleep(Duration::from_micros(HUNDRED_MILLIS_IN_MICROS + 10 * 1000))
                        .await
                });

                assert!(*flag_ptr);

                cass_future_free(cass_fut);
                let _ = Box::from_raw(flag_ptr);
            }
        }
    }
}
