use scylla::transport::errors::*;

// Re-export error types.
pub(crate) use crate::cass_error_types::{CassError, CassErrorSource};
use crate::query_error::CassErrorResult;
use crate::statement::UnknownNamedParameterError;

pub trait ToCassError {
    fn to_cass_error(&self) -> CassError;
}

impl ToCassError for CassErrorResult {
    fn to_cass_error(&self) -> CassError {
        match self {
            CassErrorResult::Query(query_error) => query_error.to_cass_error(),

            // TODO:
            // For now let's leave these as LIB_INVALID_DATA.
            // I don't see any variants that would make more sense.
            // TBH, I'm almost sure that we should introduce additional enum variants
            // of CassError in the future ~ muzarski.
            CassErrorResult::ResultMetadataLazyDeserialization(_) => {
                CassError::CASS_ERROR_LIB_INVALID_DATA
            }
            CassErrorResult::Deserialization(_) => CassError::CASS_ERROR_LIB_INVALID_DATA,
        }
    }
}

impl ToCassError for QueryError {
    fn to_cass_error(&self) -> CassError {
        match self {
            QueryError::DbError(db_error, _string) => db_error.to_cass_error(),
            QueryError::BadQuery(bad_query) => bad_query.to_cass_error(),
            QueryError::ProtocolError(_str) => CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR,
            QueryError::TimeoutError => CassError::CASS_ERROR_LIB_REQUEST_TIMED_OUT, // This may be either read or write timeout error
            QueryError::UnableToAllocStreamId => CassError::CASS_ERROR_LIB_NO_STREAMS,
            QueryError::RequestTimeout(_) => CassError::CASS_ERROR_LIB_REQUEST_TIMED_OUT,
            QueryError::CqlRequestSerialization(_) => CassError::CASS_ERROR_LIB_MESSAGE_ENCODE,
            QueryError::BodyExtensionsParseError(_) => {
                CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE
            }
            QueryError::EmptyPlan => CassError::CASS_ERROR_LIB_INVALID_STATE,
            QueryError::CqlResultParseError(_) => CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
            QueryError::CqlErrorParseError(_) => CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
            QueryError::MetadataError(_) => CassError::CASS_ERROR_LIB_INVALID_STATE,
            // I know that TranslationError (corresponding to CASS_ERROR_LIB_HOST_RESOLUTION)
            // is hidden under the ConnectionPoolError.
            // However, we still have a lot work to do when it comes to error conversion.
            // I will address it, once we start resolving all issues related to error conversion.
            QueryError::ConnectionPoolError(_) => CassError::CASS_ERROR_LIB_UNABLE_TO_CONNECT,
            QueryError::BrokenConnection(_) => CassError::CASS_ERROR_LIB_UNABLE_TO_CONNECT,
            // QueryError is non_exhaustive
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for DbError {
    fn to_cass_error(&self) -> CassError {
        match self {
            DbError::ServerError => CassError::CASS_ERROR_SERVER_SERVER_ERROR,
            DbError::ProtocolError => CassError::CASS_ERROR_SERVER_PROTOCOL_ERROR,
            DbError::AuthenticationError => CassError::CASS_ERROR_SERVER_BAD_CREDENTIALS,
            DbError::Unavailable { .. } => CassError::CASS_ERROR_SERVER_UNAVAILABLE,
            DbError::Overloaded => CassError::CASS_ERROR_SERVER_OVERLOADED,
            DbError::IsBootstrapping => CassError::CASS_ERROR_SERVER_IS_BOOTSTRAPPING,
            DbError::TruncateError => CassError::CASS_ERROR_SERVER_TRUNCATE_ERROR,
            DbError::WriteTimeout { .. } => CassError::CASS_ERROR_SERVER_WRITE_TIMEOUT,
            DbError::ReadTimeout { .. } => CassError::CASS_ERROR_SERVER_READ_TIMEOUT,
            DbError::ReadFailure { .. } => CassError::CASS_ERROR_SERVER_READ_FAILURE,
            DbError::FunctionFailure { .. } => CassError::CASS_ERROR_SERVER_FUNCTION_FAILURE,
            DbError::WriteFailure { .. } => CassError::CASS_ERROR_SERVER_WRITE_FAILURE,
            DbError::SyntaxError => CassError::CASS_ERROR_SERVER_SYNTAX_ERROR,
            DbError::Unauthorized => CassError::CASS_ERROR_SERVER_UNAUTHORIZED,
            DbError::Invalid => CassError::CASS_ERROR_SERVER_INVALID_QUERY,
            DbError::ConfigError => CassError::CASS_ERROR_SERVER_CONFIG_ERROR,
            DbError::AlreadyExists { .. } => CassError::CASS_ERROR_SERVER_ALREADY_EXISTS,
            DbError::Unprepared { .. } => CassError::CASS_ERROR_SERVER_UNPREPARED,
            DbError::Other(num) => {
                CassError((CassErrorSource::CASS_ERROR_SOURCE_SERVER.0 << 24) | *num as u32)
            }
            // TODO: add appropriate error if rate limit reached
            DbError::RateLimitReached { .. } => CassError::CASS_ERROR_SERVER_UNAVAILABLE,
        }
    }
}

impl ToCassError for BadQuery {
    fn to_cass_error(&self) -> CassError {
        match self {
            BadQuery::ValuesTooLongForKey(_usize, _usize2) => CassError::CASS_ERROR_LAST_ENTRY,
            BadQuery::BadKeyspaceName(_bad_keyspace_name) => CassError::CASS_ERROR_LAST_ENTRY,
            BadQuery::Other(_other_query) => CassError::CASS_ERROR_LAST_ENTRY,
            BadQuery::SerializationError(e) => {
                if e.downcast_ref::<UnknownNamedParameterError>().is_some() {
                    // It means that our custom `UnknownNamedParameterError` was returned.
                    CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST
                } else {
                    CassError::CASS_ERROR_LAST_ENTRY
                }
            }
            BadQuery::TooManyQueriesInBatchStatement(_) => CassError::CASS_ERROR_LAST_ENTRY,
            // BadQuery is non_exhaustive
            // For now, since all other variants return LAST_ENTRY,
            // let's do it here as well.
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for NewSessionError {
    fn to_cass_error(&self) -> CassError {
        match self {
            NewSessionError::FailedToResolveAnyHostname(_hostnames) => {
                CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE
            }
            NewSessionError::EmptyKnownNodesList => CassError::CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
            NewSessionError::DbError(_db_error, _string) => CassError::CASS_ERROR_LAST_ENTRY,
            NewSessionError::BadQuery(_bad_query) => CassError::CASS_ERROR_LAST_ENTRY,
            NewSessionError::ProtocolError(_str) => {
                CassError::CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL
            }
            NewSessionError::UnableToAllocStreamId => CassError::CASS_ERROR_LAST_ENTRY,
            NewSessionError::RequestTimeout(_) => CassError::CASS_ERROR_LIB_REQUEST_TIMED_OUT,
            NewSessionError::CqlRequestSerialization(_) => CassError::CASS_ERROR_LIB_MESSAGE_ENCODE,
            NewSessionError::BodyExtensionsParseError(_) => {
                CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE
            }
            NewSessionError::EmptyPlan => CassError::CASS_ERROR_LIB_INVALID_STATE,
            NewSessionError::CqlResultParseError(_) => {
                CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE
            }
            NewSessionError::CqlErrorParseError(_) => CassError::CASS_ERROR_LIB_UNEXPECTED_RESPONSE,
            NewSessionError::MetadataError(_) => CassError::CASS_ERROR_LIB_INVALID_STATE,
            // I know that TranslationError (corresponding to CASS_ERROR_LIB_HOST_RESOLUTION)
            // is hidden under the ConnectionPoolError.
            // However, we still have a lot work to do when it comes to error conversion.
            // I will address it, once we start resolving all issues related to error conversion.
            NewSessionError::ConnectionPoolError(_) => CassError::CASS_ERROR_LIB_UNABLE_TO_CONNECT,
            NewSessionError::BrokenConnection(_) => CassError::CASS_ERROR_LIB_UNABLE_TO_CONNECT,
            // NS error is non_exhaustive
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

impl ToCassError for BadKeyspaceName {
    fn to_cass_error(&self) -> CassError {
        match self {
            BadKeyspaceName::Empty => CassError::CASS_ERROR_LAST_ENTRY,
            BadKeyspaceName::TooLong(_string, _usize) => CassError::CASS_ERROR_LAST_ENTRY,
            BadKeyspaceName::IllegalCharacter(_string, _char) => CassError::CASS_ERROR_LAST_ENTRY,
            // non_exhaustive
            _ => CassError::CASS_ERROR_LAST_ENTRY,
        }
    }
}

pub trait CassErrorMessage {
    fn msg(&self) -> String;
}

impl CassErrorMessage for CassErrorResult {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for QueryError {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for DbError {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for BadQuery {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for NewSessionError {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for BadKeyspaceName {
    fn msg(&self) -> String {
        self.to_string()
    }
}

impl CassErrorMessage for str {
    fn msg(&self) -> String {
        self.to_string()
    }
}
