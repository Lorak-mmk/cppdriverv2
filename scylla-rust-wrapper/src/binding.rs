//! This module implements macros that generate functions used to
//! bind/append data to driver's structures (statement, UDT, tuple, collection)
//!
//! For examples on how to use this module, see documentation of `prepare_binders_macro`
//!
//! This module is a bit complicated, mostly because we can't concatenate identifiers
//! in macros, but wanted to avoid code duplication anyway (so for instance having code
//! that converts function arguments to CqlValue in only one place).
//! `make_*_binder` and `make_appender` are macros that directly declare new functions.
//! `invoke_binder_maker_macro_with_type` is responsible for converting function arguments to `Result<Option<CqlValue>, CassError>`.
//! It basically takes a macro name (`make_*_binder` / `make_appender`), some arguments,
//! and forwards those arguments to the macro, along with the description of additional arguments
//! and a function that converts them to `Result<Option<CqlValue>, CassError>`.
//! `prepare_binders_macro` declares `make_binders` macro, that is then directly called
//! by module user (more on `make_binders` in documentation of `prepare_binders_macro`).
//! Generally, `make_binders` will just use `invoke_binder_maker_macro_with_type` underneath,
//! which in turn uses `make_*_binder` / `make_appender`, which declare desired function.

//! # make_*_binder
//! make_*_binder and make_appender generate target function.
//! They should not be used directly, and are present only for internal usage by other macros.
//!
//! ## Arguments for make_*_binder and make_appender
//!
//! * `$this:ty` - type to which we are going to bind (e.g. `CassTuple`)
//! * `$consume_v:expr` - function that accepts structure reference (`&mut $this`),
//!     then optionally some flavour-specific arguments (described futher down),
//!     value (`Option<CqlValue>`), and binds value to the structure.
//! * `$fn_by_*` - name of the generated function.
//! * `$e:expr` - takes remaining function arguments and produces `Result<Option<CqlValue>, CassError>`
//! * `[$($arg:ident @ $t:ty), *]` - list of remaining function arguments and their types.
//!     Example: `[v @ *const cass_byte_t, v_size @ size_t]`
//!
//! ## Differences between make_*_binder variants
//!
//! The variants differ by arguments accepted by generated function, and directly correlate to
//! 4 types of data-binding functions encountered in cpp-driver.
//! They all take raw pointer to `mut $this`, then some arguments that tell
//! where should the value be bound (that's the part differing between them), and then
//! rest of the arguments, dictated by `[$($arg:ident @ $t:ty), *]`, that are later
//! passed to `$e`.
//!  * Function from make_index_binder takes size_t, which is and index of field
//!     where value should be bound (index of parameter in CassStatement, index of
//!     field in CassUserType, index of element in CassTuple).
//!  * Functions from make_name_binder and make_name_n_binder take a string, either
//!     as a `*const c_char` (_name version), or `*const c_char` and `size_t` (_name_n version).
//!     It can be used for binding named parameter in CassStatement or field by name in CassUserType.
//!  * Functions from make_appender don't take any extra argument, as they are for use by CassCollection
//!     functions - values are appended to collection.

macro_rules! make_index_binder {
    ($this:ty, $consume_v:expr, $fn_by_idx:ident, $e:expr, [$($arg:ident @ $t:ty), *]) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_idx(
            this: CassBorrowedMutPtr<$this>,
            index: size_t,
            $($arg: $t), *
        ) -> CassError {
            // For some reason detected as unused, which is not true
            #[allow(unused_imports)]
            use crate::value::CassCqlValue::*;
            match ($e)($($arg), *) {
                Ok(v) => $consume_v(BoxFFI::as_mut_ref( this).unwrap(), index as usize, v),
                Err(e) => e,
            }
        }
    }
}

macro_rules! make_name_binder {
    ($this:ty, $consume_v:expr, $fn_by_name:ident, $e:expr, [$($arg:ident @ $t:ty), *]) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_name(
            this: CassBorrowedMutPtr<$this>,
            name: *const c_char,
            $($arg: $t), *
        ) -> CassError {
            // For some reason detected as unused, which is not true
            #[allow(unused_imports)]
            use crate::value::CassCqlValue::*;
            let name = ptr_to_cstr(name).unwrap();
            match ($e)($($arg), *) {
                Ok(v) => $consume_v(BoxFFI::as_mut_ref( this).unwrap(), name, v),
                Err(e) => e,
            }
        }
    }
}

macro_rules! make_name_n_binder {
    ($this:ty, $consume_v:expr, $fn_by_name_n:ident, $e:expr, [$($arg:ident @ $t:ty), *]) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_name_n(
            this: CassBorrowedMutPtr<$this>,
            name: *const c_char,
            name_length: size_t,
            $($arg: $t), *
        ) -> CassError {
            // For some reason detected as unused, which is not true
            #[allow(unused_imports)]
            use crate::value::CassCqlValue::*;
            let name = ptr_to_cstr_n(name, name_length).unwrap();
            match ($e)($($arg), *) {
                Ok(v) => $consume_v(BoxFFI::as_mut_ref( this).unwrap(), name, v),
                Err(e) => e,
            }
        }
    }
}

macro_rules! make_appender {
    ($this:ty, $consume_v:expr, $fn_append:ident, $e:expr, [$($arg:ident @ $t:ty), *]) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_append(
            this: CassBorrowedMutPtr<$this>,
            $($arg: $t), *
        ) -> CassError {
            // For some reason detected as unused, which is not true
            #[allow(unused_imports)]
            use crate::value::CassCqlValue::*;
            match ($e)($($arg), *) {
                Ok(v) => $consume_v(BoxFFI::as_mut_ref( this).unwrap(), v),
                Err(e) => e,
            }
        }
    }
}

// Types for which binding is not implemented:
// custom - Not implemented in Rust driver

macro_rules! invoke_binder_maker_macro_with_type {
    (null, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!($this, $consume_v, $fn, || Ok(None), []);
    };
    (int8, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(TinyInt(v))),
            [v @ cass_int8_t]
        );
    };
    (int16, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(SmallInt(v))),
            [v @ cass_int16_t]
        );
    };
    (int32, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Int(v))),
            [v @ cass_int32_t]
        );
    };
    (uint32, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| {
                use scylla::frame::value::CqlDate;
                Ok(Some(Date(CqlDate(v))))
            },
            [v @ cass_uint32_t]
        );
    };
    (int64, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(BigInt(v))),
            [v @ cass_int64_t]
        );
    };
    (float, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Float(v))),
            [v @ cass_float_t]
        );
    };
    (double, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Double(v))),
            [v @ cass_double_t]
        );
    };
    (bool, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Boolean(v != 0))),
            [v @ cass_bool_t]
        );
    };
    (string, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Text(ptr_to_cstr(v).unwrap().to_string()))),
            [v @ *const std::os::raw::c_char]
        );
    };
    (string_n, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v, n| Ok(Some(Text(ptr_to_cstr_n(v, n).unwrap().to_string()))),
            [v @ *const std::os::raw::c_char, n @ size_t]
        );
    };
    (bytes, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v, v_size| {
                let v_vec = std::slice::from_raw_parts(v, v_size as usize).to_vec();
                Ok(Some(Blob(v_vec)))
            },
            [v @ *const cass_byte_t, v_size @ size_t]
        );
    };
    (uuid, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v: crate::uuid::CassUuid| Ok(Some(Uuid(v.into()))),
            [v @ crate::uuid::CassUuid]
        );
    };
    (inet, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v: crate::inet::CassInet| {
                // Err if length in struct is invalid.
                // cppdriver doesn't check this - it encodes any length given to it
                // but it doesn't seem like something we wanna do. Also, rust driver can't
                // really do it afaik.
                match std::convert::TryInto::try_into(v) {
                    Ok(v) => Ok(Some(Inet(v))),
                    Err(_) => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
                }
            },
            [v @ crate::inet::CassInet]
        );
    };
    (duration, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |m, d, n| {
                Ok(Some(Duration(scylla::frame::value::CqlDuration {
                    months: m,
                    days: d,
                    nanoseconds: n
                })))
            },
            [m @ cass_int32_t, d @ cass_int32_t, n @ cass_int64_t]
        );
    };
    (decimal, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v, v_size, scale| {
                use scylla::frame::value::CqlDecimal;
                let varint = std::slice::from_raw_parts(v, v_size as usize);
                Ok(Some(Decimal(CqlDecimal::from_signed_be_bytes_slice_and_exponent(varint, scale))))
            },
            [v @ *const cass_byte_t, v_size @ size_t, scale @ cass_int32_t]
        );
    };
    (collection, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |p: CassBorrowedPtr<crate::collection::CassCollection>| {
                match std::convert::TryInto::try_into(BoxFFI::as_ref(p).unwrap()) {
                    Ok(v) => Ok(Some(v)),
                    Err(_) => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
                }
            },
            [p @ CassBorrowedPtr<crate::collection::CassCollection>]
        );
    };
    (tuple, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |p: CassBorrowedPtr<crate::tuple::CassTuple>| {
                Ok(Some(BoxFFI::as_ref(p).unwrap().into()))
            },
            [p @ CassBorrowedPtr<crate::tuple::CassTuple>]
        );
    };
    (user_type, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |p: CassBorrowedPtr<crate::user_type::CassUserType>| {
                Ok(Some(BoxFFI::as_ref(p).unwrap().into()))
            },
            [p @ CassBorrowedPtr<crate::user_type::CassUserType>]
        );
    };
}

/// Usage of this macro declares a new macro - make_binders, which is then used to declare
/// functions that bind data to driver structures.
/// This macro has 3 variants - `@only_index`, `@index_and_name`, `@append`, to accomodate
/// each use case from cppdriver.
///
/// # `@only_index`
///
/// `@only_index` is used if values can only be bound to a structure by index of the field,
/// like in CassTuple. You need to pass it 2 arguments - type of the structure, and a function,
/// which accepts mut ref to the structure, index (`size_t`), value (`Option<CqlValue>`), and
/// binds the value to the structure at a given index.
///
/// After that, you can now create binders. For each supported type write: `make_binders!(type, fn_name)`.
/// For a list of supported types see `invoke_binder_maker_macro_with_type`. `fn_name` is name of created function.
/// Each function will be created using `make_index_binder` macro.
///
/// # `@index_and_name`
///
/// Use `@index_and_name` if the type supports binding both by index and by name, like CassUserType,
/// or CassStatement.
/// This version is slightly more complicated than others, and needs 3 arguments:
///  * Name of the type
///  * `$consume_v_idx:expr` - function that binds value by index - just like the one in `@only_index`
///  * `$consume_v_name:expr` - function that binds value by name.
///         Accepts mut ref to the structure, name (`str`) and value (`Option<CqlValue>`)
///
/// After that, you can again start generating functions using make_binders macro, but here it has 5 variants.
/// 3 variants are the basic ones, resembling the one from `@only_index`:
///  * `@index` - works the same as `make_binders` in `@only_index` variant - takes type and function name,
///         declares this function using `make_index_binder` macro.
///         In the function, value will be consumed using `$consume_v_idx`.
///  * `@name` - accepts type and function name, declares this function using `make_name_binder` macro.
///         In the function, value will be consumed using `$consume_v_name`.
///  * `@name_n` - accepts type and function name, declares this function using `make_name_n_binder` macro.
///         In the function, value will be consumed using `$consume_v_name`.
///         There are also 2 helper variants, to accomodate scenarios often encountered in cppdriver (sets of 3 functions,
///         binding the same type by index, name and name_n):
///  * `make_binders!(type, fn_idx, fn_name, fn_name_n)` - is equivalent to:
///     ```text
///     make_binders!(@index type, fn_idx);
///     make_binders!(@name type, fn_name);
///     make_binders!(@name_n type, fn_name_n);
///     ```
///  * `make_binders!(t1, fn_idx, t2, fn_name, t3, fn_name_n)` - is equivalent to:
///     ```text
///     make_binders!(@index t1, fn_idx);
///     make_binders!(@name t2, fn_name);
///     make_binders!(@name_n t3, fn_name_n);
///     ```
/// # `@append`
///
/// `@append` is used, when values can only be appended to structure (like in CassCollection),
/// not bound at specific index/name. You need to pass it 2 arguments - type of the structure, and a function,
/// which accepts mut ref to the structure and a value (`Option<CqlValue>`), and appends value to the structure.
///
/// After that, you can now create binders. For each supported type write: `make_binders!(type, fn_name)`.
/// For a list of supported types see `invoke_binder_maker_macro_with_type`. `fn_name` is name of created function.
/// Each function will be created using `make_appender` macro.
macro_rules! prepare_binders_macro {
    (@only_index $this:ty, $consume_v:expr) => {
        macro_rules! make_binders {
            ($t:ident, $fn:ident) => {
                invoke_binder_maker_macro_with_type!($t, make_index_binder, $this, $consume_v, $fn);
            };
        }
    };
    (@index_and_name $this:ty, $consume_v_idx:expr, $consume_v_name:expr) => {
        macro_rules! make_binders {
            ($t:ident, $fn_idx:ident, $fn_name:ident, $fn_name_n:ident) => {
                invoke_binder_maker_macro_with_type!($t, make_index_binder, $this, $consume_v_idx, $fn_idx);
                invoke_binder_maker_macro_with_type!($t, make_name_binder, $this, $consume_v_name, $fn_name);
                invoke_binder_maker_macro_with_type!($t, make_name_n_binder, $this, $consume_v_name, $fn_name_n);
            };
            ($t_idx:ident, $fn_idx:ident, $t_name:ident, $fn_name:ident, $t_name_n:ident, $fn_name_n:ident) => {
                invoke_binder_maker_macro_with_type!($t_idx, make_index_binder, $this, $consume_v_idx, $fn_idx);
                invoke_binder_maker_macro_with_type!($t_name, make_name_binder, $this, $consume_v_name, $fn_name);
                invoke_binder_maker_macro_with_type!($t_name_n, make_name_n_binder, $this, $consume_v_name, $fn_name_n);
            };
            (@index $t:ident, $fn_idx:ident) => {
                invoke_binder_maker_macro_with_type!($t, make_index_binder, $this, $consume_v_idx, $fn_idx);
            };
            (@name $t:ident, $fn_name:ident) => {
                invoke_binder_maker_macro_with_type!($t, make_name_binder, $this, $consume_v_name, $fn_name);
            };
            (@name_n $t:ident, $fn_name_n:ident) => {
                invoke_binder_maker_macro_with_type!($t, make_name_n_binder, $this, $consume_v_name, $fn_name_n);
            };
        }
    };

    (@append $this:ty, $consume_v:expr) => {
        macro_rules! make_binders {
            ($t:ident, $fn:ident) => {
                invoke_binder_maker_macro_with_type!($t, make_appender, $this, $consume_v, $fn);
            };
        }
    };
}
