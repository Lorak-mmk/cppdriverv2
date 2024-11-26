use crate::argconv::{arr_to_cstr, ptr_to_cstr, str_to_arr, CassBorrowedPtr, RefFFI};
use crate::cass_log_types::{CassLogLevel, CassLogMessage};
use crate::types::size_t;
use crate::LOGGER;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fmt::Write;
use std::os::raw::{c_char, c_void};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;
use tracing::field::Field;
use tracing::Level;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

impl RefFFI for CassLogMessage {}

pub type CassLogCallback =
    Option<unsafe extern "C" fn(message: CassBorrowedPtr<CassLogMessage>, data: *mut c_void)>;

unsafe extern "C" fn noop_log_callback(
    _message: CassBorrowedPtr<CassLogMessage>,
    _data: *mut c_void,
) {
}

pub struct Logger {
    pub cb: CassLogCallback,
    pub data: *mut c_void,
}

// The field `data` in the struct `Logger` is neither Send nor Sync.
// It can be mutated only in the user-provided `CassLogCallback`, so it is safe
// to implement Sync and Send manually for the `Logger` struct.
unsafe impl Sync for Logger {}
unsafe impl Send for Logger {}

impl From<Level> for CassLogLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::TRACE => CassLogLevel::CASS_LOG_TRACE,
            Level::DEBUG => CassLogLevel::CASS_LOG_DEBUG,
            Level::INFO => CassLogLevel::CASS_LOG_INFO,
            Level::WARN => CassLogLevel::CASS_LOG_WARN,
            Level::ERROR => CassLogLevel::CASS_LOG_ERROR,
        }
    }
}

impl TryFrom<CassLogLevel> for Level {
    type Error = String;

    fn try_from(log_level: CassLogLevel) -> Result<Self, Self::Error> {
        let level = match log_level {
            CassLogLevel::CASS_LOG_TRACE => Level::TRACE,
            CassLogLevel::CASS_LOG_DEBUG => Level::DEBUG,
            CassLogLevel::CASS_LOG_INFO => Level::INFO,
            CassLogLevel::CASS_LOG_WARN => Level::WARN,
            CassLogLevel::CASS_LOG_ERROR => Level::ERROR,
            CassLogLevel::CASS_LOG_CRITICAL => Level::ERROR,
            _ => return Err("Error while converting CassLogLevel to Level".to_string()),
        };

        Ok(level)
    }
}

pub const CASS_LOG_MAX_MESSAGE_SIZE: usize = 1024;

pub unsafe extern "C" fn stderr_log_callback(
    message: CassBorrowedPtr<CassLogMessage>,
    _data: *mut c_void,
) {
    let message = RefFFI::as_ref(message).unwrap();

    eprintln!(
        "{} [{}] ({}:{}) {}",
        message.time_ms,
        ptr_to_cstr(cass_log_level_string(message.severity)).unwrap(),
        ptr_to_cstr(message.file).unwrap(),
        message.line,
        arr_to_cstr::<{ CASS_LOG_MAX_MESSAGE_SIZE }>(&message.message).unwrap(),
    )
}

pub struct CustomLayer;

pub struct PrintlnVisitor {
    log_message: String,
}

// Collects all fields and values in a single log event into a single String
// to set into CassLogMessage::message.
impl tracing::field::Visit for PrintlnVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if self.log_message.is_empty() {
            write!(self.log_message, "{}: {:?}", field, value).unwrap();
        } else {
            write!(self.log_message, ", {}: {:?}", field, value).unwrap();
        }
    }
}

impl<S> Layer<S> for CustomLayer
where
    S: tracing::Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Current time is before UNIX_EPOCH");
        let log_time_ms = since_the_epoch.as_millis() as u64;

        let message = "";
        let mut target = event.metadata().target().to_string();
        target.push('\0');

        let mut log_message = CassLogMessage {
            time_ms: log_time_ms,
            severity: (*event.metadata().level()).into(),
            file: target.as_ptr() as *const c_char,
            line: event.metadata().line().unwrap_or(0) as i32,
            function: c"".as_ptr() as *const c_char, // ignored, as cannot be fetched from event metadata
            message: str_to_arr(message),
        };

        let mut visitor = PrintlnVisitor {
            log_message: message.to_string(),
        };
        event.record(&mut visitor);

        visitor.log_message.push('\0');
        log_message.message =
            str_to_arr::<{ CASS_LOG_MAX_MESSAGE_SIZE }>(visitor.log_message.as_str());

        let logger = LOGGER.read().unwrap();

        if let Some(log_cb) = logger.cb {
            unsafe {
                log_cb(RefFFI::as_ptr(&log_message), logger.data);
            }
        }
    }
}

// Sets tracing subscriber with specified `level`.
// The subscriber is valid for the duration of the entire program.
pub fn set_tracing_subscriber_with_level(level: Level) {
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive(level.to_owned().into()),
            )
            .with(CustomLayer),
    )
    .unwrap_or(()) // Ignore if it is set already
}

#[no_mangle]
pub unsafe extern "C" fn cass_log_set_level(log_level: CassLogLevel) {
    if log_level == CassLogLevel::CASS_LOG_DISABLED {
        debug!("Logging is disabled!");
        return;
    }

    let level = Level::try_from(log_level).unwrap_or(Level::WARN);

    // Sets the tracing subscriber with new log level.
    set_tracing_subscriber_with_level(level);
    debug!("Log level is set to {}", level);
}

#[no_mangle]
pub unsafe extern "C" fn cass_log_level_string(log_level: CassLogLevel) -> *const c_char {
    let log_level_str = match log_level {
        CassLogLevel::CASS_LOG_TRACE => c"TRACE",
        CassLogLevel::CASS_LOG_DEBUG => c"DEBUG",
        CassLogLevel::CASS_LOG_INFO => c"INFO",
        CassLogLevel::CASS_LOG_WARN => c"WARN",
        CassLogLevel::CASS_LOG_ERROR => c"ERROR",
        CassLogLevel::CASS_LOG_CRITICAL => c"CRITICAL",
        CassLogLevel::CASS_LOG_DISABLED => c"DISABLED",
        _ => c"",
    };

    log_level_str.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn cass_log_set_callback(callback: CassLogCallback, data: *mut c_void) {
    let logger = Logger {
        cb: Some(callback.unwrap_or(noop_log_callback)),
        data,
    };

    *LOGGER.write().unwrap() = logger;
}

#[no_mangle]
pub unsafe extern "C" fn cass_log_get_callback_and_data(
    callback_out: *mut CassLogCallback,
    data_out: *mut *const c_void,
) {
    let logger = LOGGER.read().unwrap();

    *callback_out = logger.cb;
    *data_out = logger.data;
}

#[no_mangle]
pub unsafe extern "C" fn cass_log_cleanup() {
    // Deprecated
}

#[no_mangle]
pub unsafe extern "C" fn cass_log_set_queue_size(_queue_size: size_t) {
    // Deprecated
}
