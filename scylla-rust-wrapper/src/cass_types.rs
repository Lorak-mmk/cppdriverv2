use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use scylla::batch::BatchType;
use scylla::frame::response::result::ColumnType;
use scylla::frame::types::{Consistency, SerialConsistency};
use scylla::transport::topology::{CollectionType, CqlType, NativeType, UserDefinedType};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

pub(crate) use crate::cass_batch_types::CassBatchType;
pub(crate) use crate::cass_consistency_types::CassConsistency;
pub(crate) use crate::cass_data_types::CassValueType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UDTDataType {
    // Vec to preserve the order of types
    pub field_types: Vec<(String, Arc<CassDataType>)>,

    pub keyspace: String,
    pub name: String,
    pub frozen: bool,
}

impl UDTDataType {
    pub fn new() -> UDTDataType {
        UDTDataType {
            field_types: Vec::new(),
            keyspace: "".to_string(),
            name: "".to_string(),
            frozen: false,
        }
    }

    pub fn create_with_params(
        user_defined_types: &HashMap<String, Arc<UserDefinedType>>,
        keyspace_name: &str,
        name: &str,
        frozen: bool,
    ) -> UDTDataType {
        UDTDataType {
            field_types: user_defined_types
                .get(name)
                .map(|udf| &udf.field_types)
                .unwrap_or(&vec![])
                .iter()
                .map(|(udt_field_name, udt_field_type)| {
                    (
                        udt_field_name.clone(),
                        Arc::new(get_column_type_from_cql_type(
                            udt_field_type,
                            user_defined_types,
                            keyspace_name,
                        )),
                    )
                })
                .collect(),
            keyspace: keyspace_name.to_string(),
            name: name.to_owned(),
            frozen,
        }
    }

    pub fn with_capacity(capacity: usize) -> UDTDataType {
        UDTDataType {
            field_types: Vec::with_capacity(capacity),
            keyspace: "".to_string(),
            name: "".to_string(),
            frozen: false,
        }
    }

    pub fn add_field(&mut self, name: String, field_type: Arc<CassDataType>) {
        self.field_types.push((name, field_type));
    }

    pub fn get_field_by_name(&self, name: &str) -> Option<&Arc<CassDataType>> {
        self.field_types
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, t)| t)
    }

    pub fn get_field_by_index(&self, index: usize) -> Option<&Arc<CassDataType>> {
        self.field_types.get(index).map(|(_, b)| b)
    }

    fn typecheck_equals(&self, other: &UDTDataType) -> bool {
        // See: https://github.com/scylladb/cpp-driver/blob/master/src/data_type.hpp#L354-L386

        if !any_string_empty_or_both_equal(&self.keyspace, &other.keyspace) {
            return false;
        }
        if !any_string_empty_or_both_equal(&self.name, &other.name) {
            return false;
        }

        // A comment from cpp-driver:
        //// UDT's can be considered equal as long as the mutual first fields shared
        //// between them are equal. UDT's are append only as far as fields go, so a
        //// newer 'version' of the UDT data type after a schema change event should be
        //// treated as equivalent in this scenario, by simply looking at the first N
        //// mutual fields they should share.
        //
        // Iterator returned from zip() is perfect for checking the first mutual fields.
        for (field, other_field) in self.field_types.iter().zip(other.field_types.iter()) {
            // Compare field names.
            if field.0 != other_field.0 {
                return false;
            }
            // Compare field types.
            if !field.1.typecheck_equals(&other_field.1) {
                return false;
            }
        }

        true
    }
}

fn any_string_empty_or_both_equal(s1: &str, s2: &str) -> bool {
    s1.is_empty() || s2.is_empty() || s1 == s2
}

impl Default for UDTDataType {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapDataType {
    Untyped,
    Key(Arc<CassDataType>),
    KeyAndValue(Arc<CassDataType>, Arc<CassDataType>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CassDataType {
    Value(CassValueType),
    UDT(UDTDataType),
    List {
        // None stands for untyped list.
        typ: Option<Arc<CassDataType>>,
        frozen: bool,
    },
    Set {
        // None stands for untyped set.
        typ: Option<Arc<CassDataType>>,
        frozen: bool,
    },
    Map {
        typ: MapDataType,
        frozen: bool,
    },
    // Empty vector stands for untyped tuple.
    Tuple(Vec<Arc<CassDataType>>),
    Custom(String),
}

impl CassDataType {
    /// Checks for equality during typechecks.
    ///
    /// This takes into account the fact that tuples/collections may be untyped.
    pub fn typecheck_equals(&self, other: &CassDataType) -> bool {
        match self {
            CassDataType::Value(t) => *t == other.get_value_type(),
            CassDataType::UDT(udt) => match other {
                CassDataType::UDT(other_udt) => udt.typecheck_equals(other_udt),
                _ => false,
            },
            CassDataType::List { typ, .. } | CassDataType::Set { typ, .. } => match other {
                CassDataType::List { typ: other_typ, .. }
                | CassDataType::Set { typ: other_typ, .. } => {
                    // If one of them is list, and the other is set, fail the typecheck.
                    if self.get_value_type() != other.get_value_type() {
                        return false;
                    }
                    match (typ, other_typ) {
                        // One of them is untyped, skip the typecheck for subtype.
                        (None, _) | (_, None) => true,
                        (Some(typ), Some(other_typ)) => typ.typecheck_equals(other_typ),
                    }
                }
                _ => false,
            },
            CassDataType::Map { typ: t, .. } => match other {
                CassDataType::Map { typ: t_other, .. } => match (t, t_other) {
                    // See https://github.com/scylladb/cpp-driver/blob/master/src/data_type.hpp#L218
                    // In cpp-driver the types are held in a vector.
                    // The logic is following:

                    // If either of vectors is empty, skip the typecheck.
                    (MapDataType::Untyped, _) => true,
                    (_, MapDataType::Untyped) => true,

                    // Otherwise, the vectors should have equal length and we perform the typecheck for subtypes.
                    (MapDataType::Key(k), MapDataType::Key(k_other)) => k.typecheck_equals(k_other),
                    (
                        MapDataType::KeyAndValue(k, v),
                        MapDataType::KeyAndValue(k_other, v_other),
                    ) => k.typecheck_equals(k_other) && v.typecheck_equals(v_other),
                    _ => false,
                },
                _ => false,
            },
            CassDataType::Tuple(sub) => match other {
                CassDataType::Tuple(other_sub) => {
                    // If either of tuples is untyped, skip the typecheck for subtypes.
                    if sub.is_empty() || other_sub.is_empty() {
                        return true;
                    }

                    // If both are non-empty, check for subtypes equality.
                    if sub.len() != other_sub.len() {
                        return false;
                    }
                    sub.iter()
                        .zip(other_sub.iter())
                        .all(|(typ, other_typ)| typ.typecheck_equals(other_typ))
                }
                _ => false,
            },
            CassDataType::Custom(_) => {
                unimplemented!("Cpp-rust-driver does not support custom types!")
            }
        }
    }
}

impl From<NativeType> for CassValueType {
    fn from(native_type: NativeType) -> CassValueType {
        match native_type {
            NativeType::Ascii => CassValueType::CASS_VALUE_TYPE_ASCII,
            NativeType::Boolean => CassValueType::CASS_VALUE_TYPE_BOOLEAN,
            NativeType::Blob => CassValueType::CASS_VALUE_TYPE_BLOB,
            NativeType::Counter => CassValueType::CASS_VALUE_TYPE_COUNTER,
            NativeType::Date => CassValueType::CASS_VALUE_TYPE_DATE,
            NativeType::Decimal => CassValueType::CASS_VALUE_TYPE_DECIMAL,
            NativeType::Double => CassValueType::CASS_VALUE_TYPE_DOUBLE,
            NativeType::Duration => CassValueType::CASS_VALUE_TYPE_DURATION,
            NativeType::Float => CassValueType::CASS_VALUE_TYPE_FLOAT,
            NativeType::Int => CassValueType::CASS_VALUE_TYPE_INT,
            NativeType::BigInt => CassValueType::CASS_VALUE_TYPE_BIGINT,
            NativeType::Text => CassValueType::CASS_VALUE_TYPE_TEXT,
            NativeType::Timestamp => CassValueType::CASS_VALUE_TYPE_TIMESTAMP,
            NativeType::Inet => CassValueType::CASS_VALUE_TYPE_INET,
            NativeType::SmallInt => CassValueType::CASS_VALUE_TYPE_SMALL_INT,
            NativeType::TinyInt => CassValueType::CASS_VALUE_TYPE_TINY_INT,
            NativeType::Time => CassValueType::CASS_VALUE_TYPE_TIME,
            NativeType::Timeuuid => CassValueType::CASS_VALUE_TYPE_TIMEUUID,
            NativeType::Uuid => CassValueType::CASS_VALUE_TYPE_UUID,
            NativeType::Varint => CassValueType::CASS_VALUE_TYPE_VARINT,
        }
    }
}

pub fn get_column_type_from_cql_type(
    cql_type: &CqlType,
    user_defined_types: &HashMap<String, Arc<UserDefinedType>>,
    keyspace_name: &str,
) -> CassDataType {
    match cql_type {
        CqlType::Native(native) => CassDataType::Value(native.clone().into()),
        CqlType::Collection { type_, frozen } => match type_ {
            CollectionType::List(list) => CassDataType::List {
                typ: Some(Arc::new(get_column_type_from_cql_type(
                    list,
                    user_defined_types,
                    keyspace_name,
                ))),
                frozen: *frozen,
            },
            CollectionType::Map(key, value) => CassDataType::Map {
                typ: MapDataType::KeyAndValue(
                    Arc::new(get_column_type_from_cql_type(
                        key,
                        user_defined_types,
                        keyspace_name,
                    )),
                    Arc::new(get_column_type_from_cql_type(
                        value,
                        user_defined_types,
                        keyspace_name,
                    )),
                ),
                frozen: *frozen,
            },
            CollectionType::Set(set) => CassDataType::Set {
                typ: Some(Arc::new(get_column_type_from_cql_type(
                    set,
                    user_defined_types,
                    keyspace_name,
                ))),
                frozen: *frozen,
            },
        },
        CqlType::Tuple(tuple) => CassDataType::Tuple(
            tuple
                .iter()
                .map(|field_type| {
                    Arc::new(get_column_type_from_cql_type(
                        field_type,
                        user_defined_types,
                        keyspace_name,
                    ))
                })
                .collect(),
        ),
        CqlType::UserDefinedType { definition, frozen } => {
            let name = match definition {
                Ok(resolved) => &resolved.name,
                Err(not_resolved) => &not_resolved.name,
            };
            CassDataType::UDT(UDTDataType::create_with_params(
                user_defined_types,
                keyspace_name,
                name,
                *frozen,
            ))
        }
    }
}

impl CassDataType {
    fn get_sub_data_type(&self, index: usize) -> Option<&Arc<CassDataType>> {
        match self {
            CassDataType::UDT(udt_data_type) => {
                udt_data_type.field_types.get(index).map(|(_, b)| b)
            }
            CassDataType::List { typ, .. } | CassDataType::Set { typ, .. } => {
                if index > 0 {
                    None
                } else {
                    typ.as_ref()
                }
            }
            CassDataType::Map {
                typ: MapDataType::Untyped,
                ..
            } => None,
            CassDataType::Map {
                typ: MapDataType::Key(k),
                ..
            } => (index == 0).then_some(k),
            CassDataType::Map {
                typ: MapDataType::KeyAndValue(k, v),
                ..
            } => match index {
                0 => Some(k),
                1 => Some(v),
                _ => None,
            },
            CassDataType::Tuple(v) => v.get(index),
            _ => None,
        }
    }

    fn add_sub_data_type(&mut self, sub_type: Arc<CassDataType>) -> Result<(), CassError> {
        match self {
            CassDataType::List { typ, .. } | CassDataType::Set { typ, .. } => match typ {
                Some(_) => Err(CassError::CASS_ERROR_LIB_BAD_PARAMS),
                None => {
                    *typ = Some(sub_type);
                    Ok(())
                }
            },
            CassDataType::Map {
                typ: MapDataType::KeyAndValue(_, _),
                ..
            } => Err(CassError::CASS_ERROR_LIB_BAD_PARAMS),
            CassDataType::Map {
                typ: MapDataType::Key(k),
                frozen,
            } => {
                *self = CassDataType::Map {
                    typ: MapDataType::KeyAndValue(k.clone(), sub_type),
                    frozen: *frozen,
                };
                Ok(())
            }
            CassDataType::Map {
                typ: MapDataType::Untyped,
                frozen,
            } => {
                *self = CassDataType::Map {
                    typ: MapDataType::Key(sub_type),
                    frozen: *frozen,
                };
                Ok(())
            }
            CassDataType::Tuple(types) => {
                types.push(sub_type);
                Ok(())
            }
            _ => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
        }
    }

    pub fn get_udt_type(&self) -> &UDTDataType {
        match self {
            CassDataType::UDT(udt) => udt,
            _ => panic!("Can get UDT out of non-UDT data type"),
        }
    }

    pub fn get_value_type(&self) -> CassValueType {
        match &self {
            CassDataType::Value(value_data_type) => *value_data_type,
            CassDataType::UDT { .. } => CassValueType::CASS_VALUE_TYPE_UDT,
            CassDataType::List { .. } => CassValueType::CASS_VALUE_TYPE_LIST,
            CassDataType::Set { .. } => CassValueType::CASS_VALUE_TYPE_SET,
            CassDataType::Map { .. } => CassValueType::CASS_VALUE_TYPE_MAP,
            CassDataType::Tuple(..) => CassValueType::CASS_VALUE_TYPE_TUPLE,
            CassDataType::Custom(..) => CassValueType::CASS_VALUE_TYPE_CUSTOM,
        }
    }
}

pub fn get_column_type(column_type: &ColumnType) -> CassDataType {
    match column_type {
        ColumnType::Custom(s) => CassDataType::Custom(s.clone().into_owned()),
        ColumnType::Ascii => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_ASCII),
        ColumnType::Boolean => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_BOOLEAN),
        ColumnType::Blob => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_BLOB),
        ColumnType::Counter => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_COUNTER),
        ColumnType::Decimal => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_DECIMAL),
        ColumnType::Date => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_DATE),
        ColumnType::Double => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_DOUBLE),
        ColumnType::Float => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_FLOAT),
        ColumnType::Int => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_INT),
        ColumnType::BigInt => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_BIGINT),
        ColumnType::Text => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_TEXT),
        ColumnType::Timestamp => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_TIMESTAMP),
        ColumnType::Inet => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_INET),
        ColumnType::Duration => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_DURATION),
        ColumnType::List(boxed_type) => CassDataType::List {
            typ: Some(Arc::new(get_column_type(boxed_type.as_ref()))),
            frozen: false,
        },
        ColumnType::Map(key, value) => CassDataType::Map {
            typ: MapDataType::KeyAndValue(
                Arc::new(get_column_type(key.as_ref())),
                Arc::new(get_column_type(value.as_ref())),
            ),
            frozen: false,
        },
        ColumnType::Set(boxed_type) => CassDataType::Set {
            typ: Some(Arc::new(get_column_type(boxed_type.as_ref()))),
            frozen: false,
        },
        ColumnType::UserDefinedType {
            type_name,
            keyspace,
            field_types,
        } => CassDataType::UDT(UDTDataType {
            field_types: field_types
                .iter()
                .map(|(name, col_type)| {
                    (
                        name.clone().into_owned(),
                        Arc::new(get_column_type(col_type)),
                    )
                })
                .collect(),
            keyspace: keyspace.clone().into_owned(),
            name: type_name.clone().into_owned(),
            frozen: false,
        }),
        ColumnType::SmallInt => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_SMALL_INT),
        ColumnType::TinyInt => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_TINY_INT),
        ColumnType::Time => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_TIME),
        ColumnType::Timeuuid => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_TIMEUUID),
        ColumnType::Tuple(v) => CassDataType::Tuple(
            v.iter()
                .map(|col_type| Arc::new(get_column_type(col_type)))
                .collect(),
        ),
        ColumnType::Uuid => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_UUID),
        ColumnType::Varint => CassDataType::Value(CassValueType::CASS_VALUE_TYPE_VARINT),
    }
}

// Changed return type to const ptr - Arc::into_raw is const.
// It's probably not a good idea - but cppdriver doesn't guarantee
// thread safety apart from CassSession and CassFuture.
// This comment also applies to other functions that create CassDataType.
#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new(value_type: CassValueType) -> *const CassDataType {
    let data_type = match value_type {
        CassValueType::CASS_VALUE_TYPE_LIST => CassDataType::List {
            typ: None,
            frozen: false,
        },
        CassValueType::CASS_VALUE_TYPE_SET => CassDataType::Set {
            typ: None,
            frozen: false,
        },
        CassValueType::CASS_VALUE_TYPE_TUPLE => CassDataType::Tuple(Vec::new()),
        CassValueType::CASS_VALUE_TYPE_MAP => CassDataType::Map {
            typ: MapDataType::Untyped,
            frozen: false,
        },
        CassValueType::CASS_VALUE_TYPE_UDT => CassDataType::UDT(UDTDataType::new()),
        CassValueType::CASS_VALUE_TYPE_CUSTOM => CassDataType::Custom("".to_string()),
        CassValueType::CASS_VALUE_TYPE_UNKNOWN => return ptr::null_mut(),
        t if t < CassValueType::CASS_VALUE_TYPE_LAST_ENTRY => CassDataType::Value(t),
        _ => return ptr::null_mut(),
    };
    Arc::into_raw(Arc::new(data_type))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new_from_existing(
    data_type: *const CassDataType,
) -> *const CassDataType {
    let data_type = ptr_to_ref(data_type);
    Arc::into_raw(Arc::new(data_type.clone()))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new_tuple(item_count: size_t) -> *const CassDataType {
    Arc::into_raw(Arc::new(CassDataType::Tuple(Vec::with_capacity(
        item_count as usize,
    ))))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_new_udt(field_count: size_t) -> *const CassDataType {
    Arc::into_raw(Arc::new(CassDataType::UDT(UDTDataType::with_capacity(
        field_count as usize,
    ))))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_free(data_type: *mut CassDataType) {
    free_arced(data_type);
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_type(data_type: *const CassDataType) -> CassValueType {
    let data_type = ptr_to_ref(data_type);
    data_type.get_value_type()
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_is_frozen(data_type: *const CassDataType) -> cass_bool_t {
    let data_type = ptr_to_ref(data_type);
    let is_frozen = match data_type {
        CassDataType::UDT(udt) => udt.frozen,
        CassDataType::List { frozen, .. } => *frozen,
        CassDataType::Set { frozen, .. } => *frozen,
        CassDataType::Map { frozen, .. } => *frozen,
        _ => false,
    };

    is_frozen as cass_bool_t
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_type_name(
    data_type: *const CassDataType,
    type_name: *mut *const c_char,
    type_name_length: *mut size_t,
) -> CassError {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::UDT(UDTDataType { name, .. }) => {
            write_str_to_c(name, type_name, type_name_length);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_type_name(
    data_type: *mut CassDataType,
    type_name: *const c_char,
) -> CassError {
    cass_data_type_set_type_name_n(data_type, type_name, strlen(type_name))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_type_name_n(
    data_type_raw: *mut CassDataType,
    type_name: *const c_char,
    type_name_length: size_t,
) -> CassError {
    let data_type = ptr_to_ref_mut(data_type_raw);
    let type_name_string = ptr_to_cstr_n(type_name, type_name_length)
        .unwrap()
        .to_string();

    match data_type {
        CassDataType::UDT(udt_data_type) => {
            udt_data_type.name = type_name_string;
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_keyspace(
    data_type: *const CassDataType,
    keyspace: *mut *const c_char,
    keyspace_length: *mut size_t,
) -> CassError {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::UDT(UDTDataType { name, .. }) => {
            write_str_to_c(name, keyspace, keyspace_length);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_keyspace(
    data_type: *mut CassDataType,
    keyspace: *const c_char,
) -> CassError {
    cass_data_type_set_keyspace_n(data_type, keyspace, strlen(keyspace))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_keyspace_n(
    data_type: *mut CassDataType,
    keyspace: *const c_char,
    keyspace_length: size_t,
) -> CassError {
    let data_type = ptr_to_ref_mut(data_type);
    let keyspace_string = ptr_to_cstr_n(keyspace, keyspace_length)
        .unwrap()
        .to_string();

    match data_type {
        CassDataType::UDT(udt_data_type) => {
            udt_data_type.keyspace = keyspace_string;
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_class_name(
    data_type: *const CassDataType,
    class_name: *mut *const ::std::os::raw::c_char,
    class_name_length: *mut size_t,
) -> CassError {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::Custom(name) => {
            write_str_to_c(name, class_name, class_name_length);
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_class_name(
    data_type: *mut CassDataType,
    class_name: *const ::std::os::raw::c_char,
) -> CassError {
    cass_data_type_set_class_name_n(data_type, class_name, strlen(class_name))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_set_class_name_n(
    data_type: *mut CassDataType,
    class_name: *const ::std::os::raw::c_char,
    class_name_length: size_t,
) -> CassError {
    let data_type = ptr_to_ref_mut(data_type);
    let class_string = ptr_to_cstr_n(class_name, class_name_length)
        .unwrap()
        .to_string();
    match data_type {
        CassDataType::Custom(name) => {
            *name = class_string;
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_type_count(data_type: *const CassDataType) -> size_t {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::Value(..) => 0,
        CassDataType::UDT(udt_data_type) => udt_data_type.field_types.len() as size_t,
        CassDataType::List { typ, .. } | CassDataType::Set { typ, .. } => typ.is_some() as size_t,
        CassDataType::Map { typ, .. } => match typ {
            MapDataType::Untyped => 0,
            MapDataType::Key(_) => 1,
            MapDataType::KeyAndValue(_, _) => 2,
        },
        CassDataType::Tuple(v) => v.len() as size_t,
        CassDataType::Custom(..) => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_sub_type_count(data_type: *const CassDataType) -> size_t {
    cass_data_type_sub_type_count(data_type)
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_data_type(
    data_type: *const CassDataType,
    index: size_t,
) -> *const CassDataType {
    let data_type = ptr_to_ref(data_type);
    let sub_type: Option<&Arc<CassDataType>> = data_type.get_sub_data_type(index as usize);

    match sub_type {
        None => std::ptr::null(),
        // Semantic from cppdriver which also returns non-owning pointer
        Some(arc) => Arc::as_ptr(arc),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_data_type_by_name(
    data_type: *const CassDataType,
    name: *const ::std::os::raw::c_char,
) -> *const CassDataType {
    cass_data_type_sub_data_type_by_name_n(data_type, name, strlen(name))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_data_type_by_name_n(
    data_type: *const CassDataType,
    name: *const ::std::os::raw::c_char,
    name_length: size_t,
) -> *const CassDataType {
    let data_type = ptr_to_ref(data_type);
    let name_str = ptr_to_cstr_n(name, name_length).unwrap();
    match data_type {
        CassDataType::UDT(udt) => match udt.get_field_by_name(name_str) {
            None => std::ptr::null(),
            Some(t) => Arc::as_ptr(t),
        },
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_sub_type_name(
    data_type: *const CassDataType,
    index: size_t,
    name: *mut *const ::std::os::raw::c_char,
    name_length: *mut size_t,
) -> CassError {
    let data_type = ptr_to_ref(data_type);
    match data_type {
        CassDataType::UDT(udt) => match udt.field_types.get(index as usize) {
            None => CassError::CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS,
            Some((field_name, _)) => {
                write_str_to_c(field_name, name, name_length);
                CassError::CASS_OK
            }
        },
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_type(
    data_type: *mut CassDataType,
    sub_data_type: *const CassDataType,
) -> CassError {
    let data_type = ptr_to_ref_mut(data_type);
    match data_type.add_sub_data_type(clone_arced(sub_data_type)) {
        Ok(()) => CassError::CASS_OK,
        Err(e) => e,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_type_by_name(
    data_type: *mut CassDataType,
    name: *const c_char,
    sub_data_type: *const CassDataType,
) -> CassError {
    cass_data_type_add_sub_type_by_name_n(data_type, name, strlen(name), sub_data_type)
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_type_by_name_n(
    data_type_raw: *mut CassDataType,
    name: *const c_char,
    name_length: size_t,
    sub_data_type_raw: *const CassDataType,
) -> CassError {
    let name_string = ptr_to_cstr_n(name, name_length).unwrap().to_string();
    let sub_data_type = clone_arced(sub_data_type_raw);

    let data_type = ptr_to_ref_mut(data_type_raw);
    match data_type {
        CassDataType::UDT(udt_data_type) => {
            // The Cpp Driver does not check whether field_types size
            // exceeded field_count.
            udt_data_type.field_types.push((name_string, sub_data_type));
            CassError::CASS_OK
        }
        _ => CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type(
    data_type: *mut CassDataType,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = Arc::new(CassDataType::Value(sub_value_type));
    cass_data_type_add_sub_type(data_type, Arc::as_ptr(&sub_data_type))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type_by_name(
    data_type: *mut CassDataType,
    name: *const c_char,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = Arc::new(CassDataType::Value(sub_value_type));
    cass_data_type_add_sub_type_by_name(data_type, name, Arc::as_ptr(&sub_data_type))
}

#[no_mangle]
pub unsafe extern "C" fn cass_data_type_add_sub_value_type_by_name_n(
    data_type: *mut CassDataType,
    name: *const c_char,
    name_length: size_t,
    sub_value_type: CassValueType,
) -> CassError {
    let sub_data_type = Arc::new(CassDataType::Value(sub_value_type));
    cass_data_type_add_sub_type_by_name_n(data_type, name, name_length, Arc::as_ptr(&sub_data_type))
}

impl TryFrom<CassConsistency> for Consistency {
    type Error = ();

    fn try_from(c: CassConsistency) -> Result<Consistency, Self::Error> {
        match c {
            CassConsistency::CASS_CONSISTENCY_ANY => Ok(Consistency::Any),
            CassConsistency::CASS_CONSISTENCY_ONE => Ok(Consistency::One),
            CassConsistency::CASS_CONSISTENCY_TWO => Ok(Consistency::Two),
            CassConsistency::CASS_CONSISTENCY_THREE => Ok(Consistency::Three),
            CassConsistency::CASS_CONSISTENCY_QUORUM => Ok(Consistency::Quorum),
            CassConsistency::CASS_CONSISTENCY_ALL => Ok(Consistency::All),
            CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM => Ok(Consistency::LocalQuorum),
            CassConsistency::CASS_CONSISTENCY_EACH_QUORUM => Ok(Consistency::EachQuorum),
            CassConsistency::CASS_CONSISTENCY_LOCAL_ONE => Ok(Consistency::LocalOne),
            CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL => Ok(Consistency::LocalSerial),
            CassConsistency::CASS_CONSISTENCY_SERIAL => Ok(Consistency::Serial),
            _ => Err(()),
        }
    }
}

impl TryFrom<CassConsistency> for SerialConsistency {
    type Error = ();

    fn try_from(serial: CassConsistency) -> Result<SerialConsistency, Self::Error> {
        match serial {
            CassConsistency::CASS_CONSISTENCY_SERIAL => Ok(SerialConsistency::Serial),
            CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL => Ok(SerialConsistency::LocalSerial),
            _ => Err(()),
        }
    }
}

pub fn make_batch_type(type_: CassBatchType) -> Option<BatchType> {
    match type_ {
        CassBatchType::CASS_BATCH_TYPE_LOGGED => Some(BatchType::Logged),
        CassBatchType::CASS_BATCH_TYPE_UNLOGGED => Some(BatchType::Unlogged),
        CassBatchType::CASS_BATCH_TYPE_COUNTER => Some(BatchType::Counter),
        _ => None,
    }
}
