use crate::cass_collection_types::CassCollectionType;
use crate::cass_error::CassError;
use crate::cass_types::{CassDataType, CassDataTypeInner, MapDataType};
use crate::types::*;
use crate::value::CassCqlValue;
use crate::{argconv::*, value};
use std::convert::TryFrom;
use std::sync::Arc;
use std::sync::LazyLock;

// These constants help us to save an allocation in case user calls `cass_collection_new` (untyped collection).
static UNTYPED_LIST_TYPE: LazyLock<Arc<CassDataType>> = LazyLock::new(|| {
    CassDataType::new_arced(CassDataTypeInner::List {
        typ: None,
        frozen: false,
    })
});
static UNTYPED_SET_TYPE: LazyLock<Arc<CassDataType>> = LazyLock::new(|| {
    CassDataType::new_arced(CassDataTypeInner::Set {
        typ: None,
        frozen: false,
    })
});
static UNTYPED_MAP_TYPE: LazyLock<Arc<CassDataType>> = LazyLock::new(|| {
    CassDataType::new_arced(CassDataTypeInner::Map {
        typ: MapDataType::Untyped,
        frozen: false,
    })
});

#[derive(Clone)]
pub struct CassCollection {
    pub collection_type: CassCollectionType,
    pub data_type: Option<Arc<CassDataType>>,
    pub capacity: usize,
    pub items: Vec<CassCqlValue>,
}

impl FFI for CassCollection {
    type Origin = FromBox;
}

impl CassCollection {
    fn typecheck_on_append(&self, value: &Option<CassCqlValue>) -> CassError {
        // See https://github.com/scylladb/cpp-driver/blob/master/src/collection.hpp#L100.
        let index = self.items.len();

        // Do validation only if it's a typed collection.
        if let Some(data_type) = &self
            .data_type
            .as_ref()
            .map(|dt| unsafe { dt.get_unchecked() })
        {
            match data_type {
                CassDataTypeInner::List { typ: subtype, .. }
                | CassDataTypeInner::Set { typ: subtype, .. } => {
                    if let Some(subtype) = subtype {
                        if !value::is_type_compatible(value, subtype) {
                            return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
                        }
                    }
                }

                CassDataTypeInner::Map { typ, .. } => {
                    // Cpp-driver does the typecheck only if both map types are present...
                    // However, we decided not to mimic this behaviour (which is probably a bug).
                    // We will do the typecheck if just the key type is defined as well (half-typed maps).
                    match typ {
                        MapDataType::Key(k_typ) => {
                            if index % 2 == 0 && !value::is_type_compatible(value, k_typ) {
                                return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
                            }
                        }
                        MapDataType::KeyAndValue(k_typ, v_typ) => {
                            if index % 2 == 0 && !value::is_type_compatible(value, k_typ) {
                                return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
                            }
                            if index % 2 != 0 && !value::is_type_compatible(value, v_typ) {
                                return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
                            }
                        }
                        // Skip the typecheck for untyped map.
                        MapDataType::Untyped => (),
                    }
                }
                _ => return CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
            }
        }

        CassError::CASS_OK
    }

    pub fn append_cql_value(&mut self, value: Option<CassCqlValue>) -> CassError {
        let err = self.typecheck_on_append(&value);
        if err != CassError::CASS_OK {
            return err;
        }
        // There is no API to append null, so unwrap is safe
        self.items.push(value.unwrap());
        CassError::CASS_OK
    }
}

impl TryFrom<&CassCollection> for CassCqlValue {
    type Error = ();
    fn try_from(collection: &CassCollection) -> Result<Self, Self::Error> {
        // FIXME: validate that collection items are correct
        let data_type = collection.data_type.clone();
        match collection.collection_type {
            CassCollectionType::CASS_COLLECTION_TYPE_LIST => Ok(CassCqlValue::List {
                data_type,
                values: collection.items.clone(),
            }),
            CassCollectionType::CASS_COLLECTION_TYPE_MAP => {
                let mut grouped_items = Vec::new();
                // FIXME: validate even number of items
                for i in (0..collection.items.len()).step_by(2) {
                    let key = collection.items[i].clone();
                    let value = collection.items[i + 1].clone();

                    grouped_items.push((key, value));
                }

                Ok(CassCqlValue::Map {
                    data_type,
                    values: grouped_items,
                })
            }
            CassCollectionType::CASS_COLLECTION_TYPE_SET => Ok(CassCqlValue::Set {
                data_type,
                values: collection.items.clone(),
            }),
            _ => Err(()),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_new(
    collection_type: CassCollectionType,
    item_count: size_t,
) -> CassOwnedMutPtr<CassCollection> {
    let capacity = match collection_type {
        // Maps consist of a key and a value, so twice
        // the number of CassCqlValue will be stored.
        CassCollectionType::CASS_COLLECTION_TYPE_MAP => item_count * 2,
        _ => item_count,
    } as usize;

    BoxFFI::into_ptr(Box::new(CassCollection {
        collection_type,
        data_type: None,
        capacity,
        items: Vec::with_capacity(capacity),
    }))
}

#[no_mangle]
unsafe extern "C" fn cass_collection_new_from_data_type(
    data_type: CassBorrowedPtr<CassDataType>,
    item_count: size_t,
) -> CassOwnedMutPtr<CassCollection> {
    let data_type = ArcFFI::cloned_from_ptr(data_type).unwrap();
    let (capacity, collection_type) = match data_type.get_unchecked() {
        CassDataTypeInner::List { .. } => {
            (item_count, CassCollectionType::CASS_COLLECTION_TYPE_LIST)
        }
        CassDataTypeInner::Set { .. } => (item_count, CassCollectionType::CASS_COLLECTION_TYPE_SET),
        // Maps consist of a key and a value, so twice
        // the number of CassCqlValue will be stored.
        CassDataTypeInner::Map { .. } => {
            (item_count * 2, CassCollectionType::CASS_COLLECTION_TYPE_MAP)
        }
        _ => return BoxFFI::null_mut(),
    };
    let capacity = capacity as usize;

    BoxFFI::into_ptr(Box::new(CassCollection {
        collection_type,
        data_type: Some(data_type),
        capacity,
        items: Vec::with_capacity(capacity),
    }))
}

#[no_mangle]
unsafe extern "C" fn cass_collection_data_type(
    collection: CassBorrowedPtr<CassCollection>,
) -> CassBorrowedPtr<CassDataType> {
    let collection_ref = BoxFFI::as_ref(collection).unwrap();

    match &collection_ref.data_type {
        Some(dt) => ArcFFI::as_ptr(dt),
        None => match collection_ref.collection_type {
            CassCollectionType::CASS_COLLECTION_TYPE_LIST => ArcFFI::as_ptr(&UNTYPED_LIST_TYPE),
            CassCollectionType::CASS_COLLECTION_TYPE_SET => ArcFFI::as_ptr(&UNTYPED_SET_TYPE),
            CassCollectionType::CASS_COLLECTION_TYPE_MAP => ArcFFI::as_ptr(&UNTYPED_MAP_TYPE),
            // CassCollectionType is a C enum. Panic, if it's out of range.
            _ => panic!(
                "CassCollectionType enum value out of range: {}",
                collection_ref.collection_type.0
            ),
        },
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_collection_free(collection: CassOwnedMutPtr<CassCollection>) {
    BoxFFI::free(collection);
}

prepare_binders_macro!(@append CassCollection, |collection: &mut CassCollection, v| collection.append_cql_value(v));
make_binders!(int8, cass_collection_append_int8);
make_binders!(int16, cass_collection_append_int16);
make_binders!(int32, cass_collection_append_int32);
make_binders!(uint32, cass_collection_append_uint32);
make_binders!(int64, cass_collection_append_int64);
make_binders!(float, cass_collection_append_float);
make_binders!(double, cass_collection_append_double);
make_binders!(bool, cass_collection_append_bool);
make_binders!(string, cass_collection_append_string);
make_binders!(string_n, cass_collection_append_string_n);
make_binders!(bytes, cass_collection_append_bytes);
make_binders!(uuid, cass_collection_append_uuid);
make_binders!(inet, cass_collection_append_inet);
make_binders!(duration, cass_collection_append_duration);
make_binders!(decimal, cass_collection_append_decimal);
make_binders!(collection, cass_collection_append_collection);
make_binders!(tuple, cass_collection_append_tuple);
make_binders!(user_type, cass_collection_append_user_type);

#[cfg(test)]
mod tests {
    use crate::{
        argconv::ArcFFI,
        cass_error::CassError,
        cass_types::{
            cass_data_type_add_sub_type, cass_data_type_free, cass_data_type_new, CassDataType,
            CassDataTypeInner, CassValueType, MapDataType,
        },
        collection::{
            cass_collection_append_double, cass_collection_append_float, cass_collection_free,
        },
        testing::assert_cass_error_eq,
    };

    use super::{
        cass_bool_t, cass_collection_append_bool, cass_collection_append_int16,
        cass_collection_data_type, cass_collection_new, cass_collection_new_from_data_type,
        CassCollectionType,
    };

    #[test]
    fn test_typecheck_on_append_to_collection() {
        unsafe {
            // untyped map (via cass_collection_new, Collection's data type is None).
            {
                let mut untyped_map =
                    cass_collection_new(CassCollectionType::CASS_COLLECTION_TYPE_MAP, 2);
                assert_cass_error_eq!(
                    cass_collection_append_bool(untyped_map.borrow_mut(), false as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_int16(untyped_map.borrow_mut(), 42),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_double(untyped_map.borrow_mut(), 42.42),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_float(untyped_map.borrow_mut(), 42.42),
                    CassError::CASS_OK
                );
                cass_collection_free(untyped_map);
            }

            // untyped map (via cass_collection_new_from_data_type - collection's type is Some(untyped_map)).
            {
                let dt = CassDataType::new_arced(CassDataTypeInner::Map {
                    typ: MapDataType::Untyped,
                    frozen: false,
                });

                let dt_ptr = ArcFFI::into_ptr(dt);
                let mut untyped_map = cass_collection_new_from_data_type(dt_ptr.borrow(), 2);

                assert_cass_error_eq!(
                    cass_collection_append_bool(untyped_map.borrow_mut(), false as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_int16(untyped_map.borrow_mut(), 42),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_double(untyped_map.borrow_mut(), 42.42),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_float(untyped_map.borrow_mut(), 42.42),
                    CassError::CASS_OK
                );
                cass_collection_free(untyped_map);
            }

            // half-typed map (key-only)
            {
                let dt = CassDataType::new_arced(CassDataTypeInner::Map {
                    typ: MapDataType::Key(CassDataType::new_arced(CassDataTypeInner::Value(
                        CassValueType::CASS_VALUE_TYPE_BOOLEAN,
                    ))),
                    frozen: false,
                });

                let dt_ptr = ArcFFI::into_ptr(dt);
                let mut half_typed_map = cass_collection_new_from_data_type(dt_ptr.borrow(), 2);

                assert_cass_error_eq!(
                    cass_collection_append_bool(half_typed_map.borrow_mut(), false as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_int16(half_typed_map.borrow_mut(), 42),
                    CassError::CASS_OK
                );

                // Second entry -> key typecheck failed.
                assert_cass_error_eq!(
                    cass_collection_append_double(half_typed_map.borrow_mut(), 42.42),
                    CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE
                );

                // Second entry -> typecheck succesful.
                assert_cass_error_eq!(
                    cass_collection_append_bool(half_typed_map.borrow_mut(), true as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_double(half_typed_map.borrow_mut(), 42.42),
                    CassError::CASS_OK
                );
                cass_collection_free(half_typed_map);
            }

            // typed map
            {
                let dt = CassDataType::new_arced(CassDataTypeInner::Map {
                    typ: MapDataType::KeyAndValue(
                        CassDataType::new_arced(CassDataTypeInner::Value(
                            CassValueType::CASS_VALUE_TYPE_BOOLEAN,
                        )),
                        CassDataType::new_arced(CassDataTypeInner::Value(
                            CassValueType::CASS_VALUE_TYPE_SMALL_INT,
                        )),
                    ),
                    frozen: false,
                });
                let dt_ptr = ArcFFI::into_ptr(dt);
                let mut bool_to_i16_map = cass_collection_new_from_data_type(dt_ptr.borrow(), 2);

                // First entry -> typecheck successful.
                assert_cass_error_eq!(
                    cass_collection_append_bool(bool_to_i16_map.borrow_mut(), false as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_int16(bool_to_i16_map.borrow_mut(), 42),
                    CassError::CASS_OK
                );

                // Second entry -> key typecheck failed.
                assert_cass_error_eq!(
                    cass_collection_append_float(bool_to_i16_map.borrow_mut(), 42.42),
                    CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE
                );

                // Third entry -> value typecheck failed.
                assert_cass_error_eq!(
                    cass_collection_append_bool(bool_to_i16_map.borrow_mut(), true as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_float(bool_to_i16_map.borrow_mut(), 42.42),
                    CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE
                );

                ArcFFI::free(dt_ptr);
                cass_collection_free(bool_to_i16_map);
            }

            // untyped set (via cass_collection_new, collection's type is None)
            {
                let mut untyped_set =
                    cass_collection_new(CassCollectionType::CASS_COLLECTION_TYPE_SET, 2);
                assert_cass_error_eq!(
                    cass_collection_append_bool(untyped_set.borrow_mut(), false as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_int16(untyped_set.borrow_mut(), 42),
                    CassError::CASS_OK
                );
                cass_collection_free(untyped_set);
            }

            // untyped set (via cass_collection_new_from_data_type, collection's type is Some(untyped_set))
            {
                let dt = CassDataType::new_arced(CassDataTypeInner::Set {
                    typ: None,
                    frozen: false,
                });

                let dt_ptr = ArcFFI::into_ptr(dt);
                let mut untyped_set = cass_collection_new_from_data_type(dt_ptr.borrow(), 2);

                assert_cass_error_eq!(
                    cass_collection_append_bool(untyped_set.borrow_mut(), false as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_int16(untyped_set.borrow_mut(), 42),
                    CassError::CASS_OK
                );
                cass_collection_free(untyped_set);
            }

            // typed set
            {
                let dt = CassDataType::new_arced(CassDataTypeInner::Set {
                    typ: Some(CassDataType::new_arced(CassDataTypeInner::Value(
                        CassValueType::CASS_VALUE_TYPE_BOOLEAN,
                    ))),
                    frozen: false,
                });
                let dt_ptr = ArcFFI::into_ptr(dt);
                let mut bool_set = cass_collection_new_from_data_type(dt_ptr.borrow(), 2);

                assert_cass_error_eq!(
                    cass_collection_append_bool(bool_set.borrow_mut(), true as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_float(bool_set.borrow_mut(), 42.42),
                    CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE
                );

                ArcFFI::free(dt_ptr);
                cass_collection_free(bool_set);
            }

            // untyped list (via cass_collection_new, collection's type is None)
            {
                let mut untyped_list =
                    cass_collection_new(CassCollectionType::CASS_COLLECTION_TYPE_LIST, 2);
                assert_cass_error_eq!(
                    cass_collection_append_bool(untyped_list.borrow_mut(), false as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_int16(untyped_list.borrow_mut(), 42),
                    CassError::CASS_OK
                );
                cass_collection_free(untyped_list);
            }

            // untyped list (via cass_collection_new_from_data_type, collection's type is Some(untyped_list))
            {
                let dt = CassDataType::new_arced(CassDataTypeInner::Set {
                    typ: None,
                    frozen: false,
                });

                let dt_ptr = ArcFFI::into_ptr(dt);
                let mut untyped_list = cass_collection_new_from_data_type(dt_ptr.borrow(), 2);

                assert_cass_error_eq!(
                    cass_collection_append_bool(untyped_list.borrow_mut(), false as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_int16(untyped_list.borrow_mut(), 42),
                    CassError::CASS_OK
                );
                cass_collection_free(untyped_list);
            }

            // typed list
            {
                let dt = CassDataType::new_arced(CassDataTypeInner::Set {
                    typ: Some(CassDataType::new_arced(CassDataTypeInner::Value(
                        CassValueType::CASS_VALUE_TYPE_BOOLEAN,
                    ))),
                    frozen: false,
                });
                let dt_ptr = ArcFFI::into_ptr(dt);
                let mut bool_list = cass_collection_new_from_data_type(dt_ptr.borrow(), 2);

                assert_cass_error_eq!(
                    cass_collection_append_bool(bool_list.borrow_mut(), true as cass_bool_t),
                    CassError::CASS_OK
                );
                assert_cass_error_eq!(
                    cass_collection_append_float(bool_list.borrow_mut(), 42.42),
                    CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE
                );

                ArcFFI::free(dt_ptr);
                cass_collection_free(bool_list);
            }
        }
    }

    #[test]
    fn regression_empty_collection_data_type_test() {
        // This is a regression test that checks whether collections return
        // an Arc-based pointer for their type, even if they are empty.
        // Previously, they would return the pointer to static data, but not Arc allocated.
        unsafe {
            let empty_list = cass_collection_new(CassCollectionType::CASS_COLLECTION_TYPE_LIST, 2);

            // This would previously return a non Arc-based pointer.
            let empty_list_dt = cass_collection_data_type(empty_list.borrow());

            let empty_set_dt = cass_data_type_new(CassValueType::CASS_VALUE_TYPE_SET);
            // This will try to increment the reference count of `empty_list_dt`.
            // Previously, this would fail, because `empty_list_dt` did not originate from an Arc allocation.
            cass_data_type_add_sub_type(empty_set_dt.borrow(), empty_list_dt);

            cass_data_type_free(empty_set_dt)
        }
    }
}
