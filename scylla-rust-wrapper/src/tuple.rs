use crate::argconv::*;
use crate::cass_error::CassError;
use crate::cass_types::CassDataType;
use crate::cass_types::CassDataTypeInner;
use crate::types::*;
use crate::value;
use crate::value::CassCqlValue;
use std::sync::Arc;
use std::sync::LazyLock;

static UNTYPED_TUPLE_TYPE: LazyLock<Arc<CassDataType>> =
    LazyLock::new(|| CassDataType::new_arced(CassDataTypeInner::Tuple(Vec::new())));

#[derive(Clone)]
pub struct CassTuple {
    pub data_type: Option<Arc<CassDataType>>,
    pub items: Vec<Option<CassCqlValue>>,
}

impl FFI for CassTuple {
    type Ownership = OwnershipExclusive;
}

impl CassTuple {
    fn get_types(&self) -> Option<&Vec<Arc<CassDataType>>> {
        match &self.data_type {
            Some(t) => match unsafe { t.as_ref().get_unchecked() } {
                CassDataTypeInner::Tuple(v) => Some(v),
                _ => unreachable!(),
            },
            None => None,
        }
    }

    // Logic in this function is adapted from cppdriver.
    // https://github.com/scylladb/cpp-driver/blob/81ac12845bdb02f43bbf0384107334e37ae57cde/src/tuple.hpp#L87-L99
    // If the tuple was created without type (`cass_tuple_new`) we'll only make sure to
    // not bind items with too high index.
    // If it was created using `cass_tuple_new_from_data_type` we additionally check if
    // value has correct type.
    fn bind_value(&mut self, index: usize, v: Option<CassCqlValue>) -> CassError {
        if index >= self.items.len() {
            return CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
        }

        if let Some(inner_types) = self.get_types() {
            if !value::is_type_compatible(&v, &inner_types[index]) {
                return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
            }
        }

        self.items[index] = v;

        CassError::CASS_OK
    }
}

impl From<&CassTuple> for CassCqlValue {
    fn from(tuple: &CassTuple) -> Self {
        CassCqlValue::Tuple {
            data_type: tuple.data_type.clone(),
            fields: tuple.items.clone(),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_tuple_new(item_count: size_t) -> CassExclusiveMutPtr<CassTuple> {
    BoxFFI::into_ptr(Box::new(CassTuple {
        data_type: None,
        items: vec![None; item_count as usize],
    }))
}

#[no_mangle]
unsafe extern "C" fn cass_tuple_new_from_data_type(
    data_type: CassSharedPtr<CassDataType>,
) -> CassExclusiveMutPtr<CassTuple> {
    let data_type = ArcFFI::cloned_from_ptr(data_type).unwrap();
    let item_count = match data_type.get_unchecked() {
        CassDataTypeInner::Tuple(v) => v.len(),
        _ => return BoxFFI::null_mut(),
    };
    BoxFFI::into_ptr(Box::new(CassTuple {
        data_type: Some(data_type),
        items: vec![None; item_count],
    }))
}

#[no_mangle]
unsafe extern "C" fn cass_tuple_free(tuple: CassExclusiveMutPtr<CassTuple>) {
    BoxFFI::free(tuple);
}

#[no_mangle]
unsafe extern "C" fn cass_tuple_data_type(
    tuple: CassExclusiveConstPtr<CassTuple>,
) -> CassSharedPtr<CassDataType> {
    match &BoxFFI::as_ref(&tuple).unwrap().data_type {
        Some(t) => ArcFFI::as_ptr(t),
        None => ArcFFI::as_ptr(&UNTYPED_TUPLE_TYPE),
    }
}

prepare_binders_macro!(@only_index CassTuple, |tuple: &mut CassTuple, index, v| tuple.bind_value(index, v));
make_binders!(null, cass_tuple_set_null);
make_binders!(int8, cass_tuple_set_int8);
make_binders!(int16, cass_tuple_set_int16);
make_binders!(int32, cass_tuple_set_int32);
make_binders!(uint32, cass_tuple_set_uint32);
make_binders!(int64, cass_tuple_set_int64);
make_binders!(float, cass_tuple_set_float);
make_binders!(double, cass_tuple_set_double);
make_binders!(bool, cass_tuple_set_bool);
make_binders!(string, cass_tuple_set_string);
make_binders!(string_n, cass_tuple_set_string_n);
make_binders!(bytes, cass_tuple_set_bytes);
make_binders!(uuid, cass_tuple_set_uuid);
make_binders!(inet, cass_tuple_set_inet);
make_binders!(duration, cass_tuple_set_duration);
make_binders!(decimal, cass_tuple_set_decimal);
make_binders!(collection, cass_tuple_set_collection);
make_binders!(tuple, cass_tuple_set_tuple);
make_binders!(user_type, cass_tuple_set_user_type);

#[cfg(test)]
mod tests {
    use crate::cass_types::{
        cass_data_type_add_sub_type, cass_data_type_free, cass_data_type_new, CassValueType,
    };

    use super::{cass_tuple_data_type, cass_tuple_new};

    #[test]
    fn regression_empty_tuple_data_type_test() {
        // This is a regression test that checks whether tuples return
        // an Arc-based pointer for their type, even if they are empty.
        // Previously, they would return the pointer to static data, but not Arc allocated.
        unsafe {
            let empty_tuple = cass_tuple_new(2);

            // This would previously return a non Arc-based pointer.
            let empty_tuple_dt = cass_tuple_data_type(empty_tuple.into_const());

            let empty_set_dt = cass_data_type_new(CassValueType::CASS_VALUE_TYPE_SET);
            // This will try to increment the reference count of `empty_tuple_dt`.
            // Previously, this would fail, because `empty_tuple_dt` did not originate from an Arc allocation.
            cass_data_type_add_sub_type(empty_set_dt.clone(), empty_tuple_dt);

            cass_data_type_free(empty_set_dt)
        }
    }
}
