use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_error::CassErrorMessage;
use crate::prepared::CassPrepared;
use crate::query_error::CassErrorResult;
use crate::query_result::CassResult;
use crate::types::*;
use crate::uuid::CassUuid;
use crate::RUNTIME;
use scylla::prepared_statement::PreparedStatement;
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
    Prepared(Arc<PreparedStatement>),
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
    fn invoke(self, fut: &CassFuture) {
        unsafe {
            self.cb.unwrap()(fut as *const CassFuture, self.data);
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

impl CassFuture {
    pub fn make_raw(
        fut: impl Future<Output = CassFutureResult> + Send + 'static,
    ) -> *mut CassFuture {
        Self::new_from_future(fut).into_raw() as *mut _
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
                bound_cb.invoke(cass_fut_clone.as_ref());
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

    fn with_waited_state<T>(&self, f: impl FnOnce(&mut CassFutureState) -> T) -> T {
        let mut guard = self.state.lock().unwrap();
        let handle = guard.join_handle.take();
        if let Some(handle) = handle {
            mem::drop(guard);
            RUNTIME.block_on(handle).unwrap();
            guard = self.state.lock().unwrap();
        } else {
            guard = self
                .wait_for_value
                .wait_while(guard, |state| state.value.is_none())
                .unwrap();
        }
        f(&mut guard)
    }

    fn with_waited_result_timed<T>(
        &self,
        f: impl FnOnce(&mut CassFutureResult) -> T,
        timeout_duration: Duration,
    ) -> Result<T, ()> {
        self.with_waited_state_timed(|s| f(s.value.as_mut().unwrap()), timeout_duration)
    }

    pub(self) fn with_waited_state_timed<T>(
        &self,
        f: impl FnOnce(&mut CassFutureState) -> T,
        timeout_duration: Duration,
    ) -> Result<T, ()> {
        let mut guard = self.state.lock().unwrap();
        let handle = guard.join_handle.take();
        if let Some(handle) = handle {
            mem::drop(guard);
            // Need to wrap it with async{} block, so the timeout is lazily executed inside the runtime.
            // See mention about panics: https://docs.rs/tokio/latest/tokio/time/fn.timeout.html.
            let timed = async { tokio::time::timeout(timeout_duration, handle).await };
            RUNTIME.block_on(timed).map_err(|_| ())?.unwrap();
            guard = self.state.lock().unwrap();
        } else {
            let (guard_result, timeout_result) = self
                .wait_for_value
                .wait_timeout_while(guard, timeout_duration, |state| state.value.is_none())
                .unwrap();
            if timeout_result.timed_out() {
                return Err(());
            }
            guard = guard_result;
        }

        Ok(f(&mut guard))
    }

    pub fn set_callback(&self, cb: CassFutureCallback, data: *mut c_void) -> CassError {
        let mut lock = self.state.lock().unwrap();
        if lock.callback.is_some() {
            // Another callback has been already set
            return CassError::CASS_ERROR_LIB_CALLBACK_ALREADY_SET;
        }
        let bound_cb = BoundCallback { cb, data };
        if lock.value.is_some() {
            // The value is already available, we need to call the callback ourselves
            mem::drop(lock);
            bound_cb.invoke(self);
            return CassError::CASS_OK;
        }
        // Store the callback
        lock.callback = Some(bound_cb);
        CassError::CASS_OK
    }

    fn into_raw(self: Arc<Self>) -> *const Self {
        Arc::into_raw(self)
    }
}

// Do not remove; this asserts that `CassFuture` implements Send + Sync,
// which is required by the cpp-driver (saying that `CassFuture` is thread-safe).
#[allow(unused)]
trait CheckSendSync: Send + Sync {}
impl CheckSendSync for CassFuture {}

#[no_mangle]
pub unsafe extern "C" fn cass_future_set_callback(
    future_raw: *const CassFuture,
    callback: CassFutureCallback,
    data: *mut ::std::os::raw::c_void,
) -> CassError {
    ptr_to_ref(future_raw).set_callback(callback, data)
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_wait(future_raw: *const CassFuture) {
    ptr_to_ref(future_raw).with_waited_result(|_| ());
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_wait_timed(
    future_raw: *const CassFuture,
    timeout_us: cass_duration_t,
) -> cass_bool_t {
    ptr_to_ref(future_raw)
        .with_waited_result_timed(|_| (), Duration::from_micros(timeout_us))
        .is_ok() as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_ready(future_raw: *const CassFuture) -> cass_bool_t {
    let state_guard = ptr_to_ref(future_raw).state.lock().unwrap();
    match state_guard.value {
        None => cass_false,
        Some(_) => cass_true,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_error_code(future_raw: *const CassFuture) -> CassError {
    ptr_to_ref(future_raw).with_waited_result(|r: &mut CassFutureResult| match r {
        Ok(CassResultValue::QueryError(err)) => CassError::from(err.as_ref()),
        Err((err, _)) => *err,
        _ => CassError::CASS_OK,
    })
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_error_message(
    future: *mut CassFuture,
    message: *mut *const ::std::os::raw::c_char,
    message_length: *mut size_t,
) {
    ptr_to_ref(future).with_waited_state(|state: &mut CassFutureState| {
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
pub unsafe extern "C" fn cass_future_free(future_raw: *const CassFuture) {
    free_arced(future_raw);
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_get_result(
    future_raw: *const CassFuture,
) -> *const CassResult {
    ptr_to_ref(future_raw)
        .with_waited_result(|r: &mut CassFutureResult| -> Option<Arc<CassResult>> {
            match r.as_ref().ok()? {
                CassResultValue::QueryResult(qr) => Some(qr.clone()),
                _ => None,
            }
        })
        .map_or(std::ptr::null(), Arc::into_raw)
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_get_error_result(
    future_raw: *const CassFuture,
) -> *const CassErrorResult {
    ptr_to_ref(future_raw)
        .with_waited_result(|r: &mut CassFutureResult| -> Option<Arc<CassErrorResult>> {
            match r.as_ref().ok()? {
                CassResultValue::QueryError(qr) => Some(qr.clone()),
                _ => None,
            }
        })
        .map_or(std::ptr::null(), Arc::into_raw)
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_get_prepared(
    future_raw: *mut CassFuture,
) -> *const CassPrepared {
    ptr_to_ref(future_raw)
        .with_waited_result(|r: &mut CassFutureResult| -> Option<Arc<CassPrepared>> {
            match r.as_ref().ok()? {
                CassResultValue::Prepared(p) => Some(p.clone()),
                _ => None,
            }
        })
        .map_or(std::ptr::null(), Arc::into_raw)
}

#[no_mangle]
pub unsafe extern "C" fn cass_future_tracing_id(
    future: *const CassFuture,
    tracing_id: *mut CassUuid,
) -> CassError {
    ptr_to_ref(future).with_waited_result(|r: &mut CassFutureResult| match r {
        Ok(CassResultValue::QueryResult(result)) => match result.metadata.tracing_id {
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
    use crate::testing::assert_cass_future_error_message_eq;

    use super::*;
    use std::{os::raw::c_char, thread, time::Duration};

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

        struct PtrWrapper(*mut CassFuture);
        unsafe impl Send for PtrWrapper {}
        let wrapped_cass_fut = PtrWrapper(cass_fut);
        unsafe {
            let handle = thread::spawn(move || {
                let PtrWrapper(cass_fut) = wrapped_cass_fut;
                assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));
            });

            assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));

            handle.join().unwrap();
            cass_future_free(cass_fut);
        }
    }

    // This test makes sure that the future resolves even if timeout happens.
    //
    // Tokio's runtime documentation states that Runtime::spawn starts
    // running a future immediately, even if the future was not await'ed before.
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
            let timed_result = cass_future_wait_timed(cass_fut, HUNDRED_MILLIS_IN_MICROS / 5);
            assert_eq!(0, timed_result);

            // This should timeout on cond variable (previous call consumed JoinHandle).
            let timed_result = cass_future_wait_timed(cass_fut, HUNDRED_MILLIS_IN_MICROS / 5);
            assert_eq!(0, timed_result);

            // Verify that future eventually resolves, even though timeouts occurred before.
            assert_cass_future_error_message_eq!(cass_fut, Some(ERROR_MSG));

            cass_future_free(cass_fut);
        }
    }
}
