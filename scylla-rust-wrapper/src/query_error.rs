use crate::argconv::*;
use crate::cass_error::*;
use crate::cass_error_types::CassWriteType;
use crate::cass_types::CassConsistency;
use crate::types::*;
use scylla::deserialize::DeserializationError;
use scylla::frame::frame_errors::ResultMetadataAndRowsCountParseError;
use scylla::statement::Consistency;
use scylla::transport::errors::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CassErrorResult {
    #[error(transparent)]
    Query(#[from] QueryError),
    #[error(transparent)]
    ResultMetadataLazyDeserialization(#[from] ResultMetadataAndRowsCountParseError),
    #[error("Failed to deserialize first row: {0}")]
    Deserialization(#[from] DeserializationError),
}

impl FFI for CassErrorResult {
    type Ownership = OwnershipShared;
}

impl From<Consistency> for CassConsistency {
    fn from(c: Consistency) -> CassConsistency {
        match c {
            Consistency::Any => CassConsistency::CASS_CONSISTENCY_ANY,
            Consistency::One => CassConsistency::CASS_CONSISTENCY_ONE,
            Consistency::Two => CassConsistency::CASS_CONSISTENCY_TWO,
            Consistency::Three => CassConsistency::CASS_CONSISTENCY_THREE,
            Consistency::Quorum => CassConsistency::CASS_CONSISTENCY_QUORUM,
            Consistency::All => CassConsistency::CASS_CONSISTENCY_ALL,
            Consistency::LocalQuorum => CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM,
            Consistency::EachQuorum => CassConsistency::CASS_CONSISTENCY_EACH_QUORUM,
            Consistency::LocalOne => CassConsistency::CASS_CONSISTENCY_LOCAL_ONE,
            Consistency::Serial => CassConsistency::CASS_CONSISTENCY_SERIAL,
            Consistency::LocalSerial => CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL,
        }
    }
}

impl From<&WriteType> for CassWriteType {
    fn from(c: &WriteType) -> CassWriteType {
        match c {
            WriteType::Simple => CassWriteType::CASS_WRITE_TYPE_SIMPLE,
            WriteType::Batch => CassWriteType::CASS_WRITE_TYPE_BATCH,
            WriteType::UnloggedBatch => CassWriteType::CASS_WRITE_TYPE_UNLOGGED_BATCH,
            WriteType::Counter => CassWriteType::CASS_WRITE_TYPE_COUNTER,
            WriteType::BatchLog => CassWriteType::CASS_WRITE_TYPE_BATCH_LOG,
            WriteType::Cas => CassWriteType::CASS_WRITE_TYPE_CAS,
            WriteType::View => CassWriteType::CASS_WRITE_TYPE_VIEW,
            WriteType::Cdc => CassWriteType::CASS_WRITE_TYPE_CDC,
            WriteType::Other(_) => CassWriteType::CASS_WRITE_TYPE_UNKNOWN,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_free(error_result: CassSharedPtr<CassErrorResult>) {
    ArcFFI::free(error_result);
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_code(
    error_result: CassSharedPtr<CassErrorResult>,
) -> CassError {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    error_result.to_cass_error()
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_consistency(
    error_result: CassSharedPtr<CassErrorResult>,
) -> CassConsistency {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(
            DbError::Unavailable { consistency, .. },
            _,
        )) => CassConsistency::from(*consistency),
        CassErrorResult::Query(QueryError::DbError(
            DbError::ReadTimeout { consistency, .. },
            _,
        )) => CassConsistency::from(*consistency),
        CassErrorResult::Query(QueryError::DbError(
            DbError::WriteTimeout { consistency, .. },
            _,
        )) => CassConsistency::from(*consistency),
        CassErrorResult::Query(QueryError::DbError(
            DbError::ReadFailure { consistency, .. },
            _,
        )) => CassConsistency::from(*consistency),
        CassErrorResult::Query(QueryError::DbError(
            DbError::WriteFailure { consistency, .. },
            _,
        )) => CassConsistency::from(*consistency),
        _ => CassConsistency::CASS_CONSISTENCY_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_responses_received(
    error_result: CassSharedPtr<CassErrorResult>,
) -> cass_int32_t {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(DbError::Unavailable { alive, .. }, _)) => {
            *alive
        }
        CassErrorResult::Query(QueryError::DbError(DbError::ReadTimeout { received, .. }, _)) => {
            *received
        }
        CassErrorResult::Query(QueryError::DbError(DbError::WriteTimeout { received, .. }, _)) => {
            *received
        }
        CassErrorResult::Query(QueryError::DbError(DbError::ReadFailure { received, .. }, _)) => {
            *received
        }
        CassErrorResult::Query(QueryError::DbError(DbError::WriteFailure { received, .. }, _)) => {
            *received
        }
        _ => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_responses_required(
    error_result: CassSharedPtr<CassErrorResult>,
) -> cass_int32_t {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(DbError::Unavailable { required, .. }, _)) => {
            *required
        }
        CassErrorResult::Query(QueryError::DbError(DbError::ReadTimeout { required, .. }, _)) => {
            *required
        }
        CassErrorResult::Query(QueryError::DbError(DbError::WriteTimeout { required, .. }, _)) => {
            *required
        }
        CassErrorResult::Query(QueryError::DbError(DbError::ReadFailure { required, .. }, _)) => {
            *required
        }
        CassErrorResult::Query(QueryError::DbError(DbError::WriteFailure { required, .. }, _)) => {
            *required
        }
        _ => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_num_failures(
    error_result: CassSharedPtr<CassErrorResult>,
) -> cass_int32_t {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(
            DbError::ReadFailure { numfailures, .. },
            _,
        )) => *numfailures,
        CassErrorResult::Query(QueryError::DbError(
            DbError::WriteFailure { numfailures, .. },
            _,
        )) => *numfailures,
        _ => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_data_present(
    error_result: CassSharedPtr<CassErrorResult>,
) -> cass_bool_t {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(
            DbError::ReadTimeout { data_present, .. },
            _,
        )) => {
            if *data_present {
                cass_true
            } else {
                cass_false
            }
        }
        CassErrorResult::Query(QueryError::DbError(
            DbError::ReadFailure { data_present, .. },
            _,
        )) => {
            if *data_present {
                cass_true
            } else {
                cass_false
            }
        }
        _ => cass_false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_write_type(
    error_result: CassSharedPtr<CassErrorResult>,
) -> CassWriteType {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(
            DbError::WriteTimeout { write_type, .. },
            _,
        )) => CassWriteType::from(write_type),
        CassErrorResult::Query(QueryError::DbError(
            DbError::WriteFailure { write_type, .. },
            _,
        )) => CassWriteType::from(write_type),
        _ => CassWriteType::CASS_WRITE_TYPE_UNKNOWN,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_keyspace(
    error_result: CassSharedPtr<CassErrorResult>,
    c_keyspace: *mut *const ::std::os::raw::c_char,
    c_keyspace_len: *mut size_t,
) -> CassError {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(DbError::AlreadyExists { keyspace, .. }, _)) => {
            write_str_to_c(keyspace.as_str(), c_keyspace, c_keyspace_len);
            CassError::CASS_OK
        }
        CassErrorResult::Query(QueryError::DbError(
            DbError::FunctionFailure { keyspace, .. },
            _,
        )) => {
            write_str_to_c(keyspace.as_str(), c_keyspace, c_keyspace_len);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_table(
    error_result: CassSharedPtr<CassErrorResult>,
    c_table: *mut *const ::std::os::raw::c_char,
    c_table_len: *mut size_t,
) -> CassError {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(DbError::AlreadyExists { table, .. }, _)) => {
            write_str_to_c(table.as_str(), c_table, c_table_len);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_function(
    error_result: CassSharedPtr<CassErrorResult>,
    c_function: *mut *const ::std::os::raw::c_char,
    c_function_len: *mut size_t,
) -> CassError {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(
            DbError::FunctionFailure { function, .. },
            _,
        )) => {
            write_str_to_c(function.as_str(), c_function, c_function_len);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_num_arg_types(
    error_result: CassSharedPtr<CassErrorResult>,
) -> size_t {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(
            DbError::FunctionFailure { arg_types, .. },
            _,
        )) => arg_types.len() as size_t,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_error_result_arg_type(
    error_result: CassSharedPtr<CassErrorResult>,
    index: size_t,
    arg_type: *mut *const ::std::os::raw::c_char,
    arg_type_length: *mut size_t,
) -> CassError {
    let error_result: &CassErrorResult = ArcFFI::as_ref(&error_result).unwrap();
    match error_result {
        CassErrorResult::Query(QueryError::DbError(
            DbError::FunctionFailure { arg_types, .. },
            _,
        )) => {
            if index >= arg_types.len() as size_t {
                return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
            }
            write_str_to_c(
                arg_types[index as usize].as_str(),
                arg_type,
                arg_type_length,
            );
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE,
    }
}
