use crate::cass_error::CassError;
use crate::cass_types::{CassDataType, CassDataTypeInner};
use crate::types::*;
use crate::value::CassCqlValue;
use crate::{argconv::*, value};
use std::os::raw::c_char;
use std::sync::Arc;

#[derive(Clone)]
pub struct CassUserType {
    pub data_type: Arc<CassDataType>,

    // Vec to preserve the order of fields
    pub field_values: Vec<Option<CassCqlValue>>,
}

impl BoxFFI for CassUserType {}

impl CassUserType {
    fn set_field_by_index(&mut self, index: usize, value: Option<CassCqlValue>) -> CassError {
        if index >= self.field_values.len() {
            return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
        }
        if !value::is_type_compatible(&value, unsafe {
            &self.data_type.get_unchecked().get_udt_type().field_types[index].1
        }) {
            return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
        }
        self.field_values[index] = value;
        CassError::CASS_OK
    }

    fn set_field_by_name(&mut self, name: &str, value: Option<CassCqlValue>) -> CassError {
        let mut found_field: bool = false;
        for (index, (field_name, field_type)) in unsafe {
            self.data_type
                .get_unchecked()
                .get_udt_type()
                .field_types
                .iter()
                .enumerate()
        } {
            if *field_name == name {
                found_field = true;
                if index >= self.field_values.len() {
                    return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
                }
                if !value::is_type_compatible(&value, field_type) {
                    return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
                }
                self.field_values[index].clone_from(&value);
            }
        }

        if found_field {
            CassError::CASS_OK
        } else {
            CassError::CASS_ERROR_LIB_NAME_DOES_NOT_EXIST
        }
    }
}

impl From<&CassUserType> for CassCqlValue {
    fn from(user_type: &CassUserType) -> Self {
        CassCqlValue::UserDefinedType {
            data_type: user_type.data_type.clone(),
            fields: user_type
                .field_values
                .iter()
                .zip(unsafe {
                    user_type
                        .data_type
                        .get_unchecked()
                        .get_udt_type()
                        .field_types
                        .iter()
                })
                .map(|(v, (name, _))| (name.clone(), v.clone()))
                .collect(),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_new_from_data_type(
    data_type_raw: CassBorrowedPtr<CassDataType>,
) -> CassOwnedMutPtr<CassUserType> {
    let data_type = ArcFFI::cloned_from_ptr(data_type_raw).unwrap();

    match data_type.get_unchecked() {
        CassDataTypeInner::UDT(udt_data_type) => {
            let field_values = vec![None; udt_data_type.field_types.len()];
            BoxFFI::into_ptr(Box::new(CassUserType {
                data_type,
                field_values,
            }))
        }
        _ => BoxFFI::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_user_type_free(user_type: CassOwnedMutPtr<CassUserType>) {
    BoxFFI::free(user_type);
}
#[no_mangle]
pub unsafe extern "C" fn cass_user_type_data_type(
    user_type: CassBorrowedPtr<CassUserType>,
) -> CassBorrowedPtr<CassDataType> {
    ArcFFI::as_ptr(&BoxFFI::as_ref(user_type).unwrap().data_type)
}

prepare_binders_macro!(@index_and_name CassUserType,
    |udt: &mut CassUserType, index, v| udt.set_field_by_index(index, v),
    |udt: &mut CassUserType, name, v| udt.set_field_by_name(name, v));

make_binders!(
    null,
    cass_user_type_set_null,
    cass_user_type_set_null_by_name,
    cass_user_type_set_null_by_name_n
);
make_binders!(
    int8,
    cass_user_type_set_int8,
    cass_user_type_set_int8_by_name,
    cass_user_type_set_int8_by_name_n
);
make_binders!(
    int16,
    cass_user_type_set_int16,
    cass_user_type_set_int16_by_name,
    cass_user_type_set_int16_by_name_n
);
make_binders!(
    int32,
    cass_user_type_set_int32,
    cass_user_type_set_int32_by_name,
    cass_user_type_set_int32_by_name_n
);
make_binders!(
    uint32,
    cass_user_type_set_uint32,
    cass_user_type_set_uint32_by_name,
    cass_user_type_set_uint32_by_name_n
);
make_binders!(
    int64,
    cass_user_type_set_int64,
    cass_user_type_set_int64_by_name,
    cass_user_type_set_int64_by_name_n
);
make_binders!(
    float,
    cass_user_type_set_float,
    cass_user_type_set_float_by_name,
    cass_user_type_set_float_by_name_n
);
make_binders!(
    double,
    cass_user_type_set_double,
    cass_user_type_set_double_by_name,
    cass_user_type_set_double_by_name_n
);
make_binders!(
    bool,
    cass_user_type_set_bool,
    cass_user_type_set_bool_by_name,
    cass_user_type_set_bool_by_name_n
);
make_binders!(
    string,
    cass_user_type_set_string,
    string,
    cass_user_type_set_string_by_name,
    string_n,
    cass_user_type_set_string_by_name_n
);
make_binders!(@index string_n, cass_user_type_set_string_n);
make_binders!(
    bytes,
    cass_user_type_set_bytes,
    cass_user_type_set_bytes_by_name,
    cass_user_type_set_bytes_by_name_n
);
make_binders!(
    uuid,
    cass_user_type_set_uuid,
    cass_user_type_set_uuid_by_name,
    cass_user_type_set_uuid_by_name_n
);
make_binders!(
    inet,
    cass_user_type_set_inet,
    cass_user_type_set_inet_by_name,
    cass_user_type_set_inet_by_name_n
);
make_binders!(
    duration,
    cass_user_type_set_duration,
    cass_user_type_set_duration_by_name,
    cass_user_type_set_duration_by_name_n
);
make_binders!(
    decimal,
    cass_user_type_set_decimal,
    cass_user_type_set_decimal_by_name,
    cass_user_type_set_decimal_by_name_n
);
make_binders!(
    collection,
    cass_user_type_set_collection,
    cass_user_type_set_collection_by_name,
    cass_user_type_set_collection_by_name_n
);
make_binders!(
    tuple,
    cass_user_type_set_tuple,
    cass_user_type_set_tuple_by_name,
    cass_user_type_set_tuple_by_name_n
);
make_binders!(
    user_type,
    cass_user_type_set_user_type,
    cass_user_type_set_user_type_by_name,
    cass_user_type_set_user_type_by_name_n
);
