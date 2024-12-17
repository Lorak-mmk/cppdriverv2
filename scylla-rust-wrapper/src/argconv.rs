use crate::types::size_t;
use std::cmp::min;
use std::ffi::CStr;
use std::marker::PhantomData;
use std::os::raw::c_char;
use std::ptr::NonNull;
use std::sync::Arc;

pub unsafe fn ptr_to_cstr(ptr: *const c_char) -> Option<&'static str> {
    CStr::from_ptr(ptr).to_str().ok()
}

pub unsafe fn ptr_to_cstr_n(ptr: *const c_char, size: size_t) -> Option<&'static str> {
    if ptr.is_null() {
        return None;
    }
    std::str::from_utf8(std::slice::from_raw_parts(ptr as *const u8, size as usize)).ok()
}

pub unsafe fn arr_to_cstr<const N: usize>(arr: &[c_char]) -> Option<&'static str> {
    let null_char = '\0' as c_char;
    let end_index = arr[..N].iter().position(|c| c == &null_char).unwrap_or(N);
    ptr_to_cstr_n(arr.as_ptr(), end_index as size_t)
}

pub fn str_to_arr<const N: usize>(s: &str) -> [c_char; N] {
    let mut result = ['\0' as c_char; N];

    // Max length must be null-terminated
    let mut max_len = min(N - 1, s.as_bytes().len());

    while !s.is_char_boundary(max_len) {
        max_len -= 1;
    }

    for (i, c) in s.as_bytes().iter().enumerate().take(max_len) {
        result[i] = *c as c_char;
    }

    result
}

pub unsafe fn write_str_to_c(s: &str, c_str: *mut *const c_char, c_strlen: *mut size_t) {
    *c_str = s.as_ptr() as *const c_char;
    *c_strlen = s.len() as u64;
}

pub unsafe fn strlen(ptr: *const c_char) -> size_t {
    if ptr.is_null() {
        return 0;
    }
    libc::strlen(ptr) as size_t
}

#[cfg(test)]
pub fn str_to_c_str_n(s: &str) -> (*const c_char, size_t) {
    let mut c_str = std::ptr::null();
    let mut c_strlen = size_t::default();

    // SAFETY: The pointers that are passed to `write_str_to_c` are compile-checked references.
    unsafe { write_str_to_c(s, &mut c_str, &mut c_strlen) };

    (c_str, c_strlen)
}

#[cfg(test)]
macro_rules! make_c_str {
    ($str:literal) => {
        concat!($str, "\0").as_ptr() as *const c_char
    };
}

#[cfg(test)]
pub(crate) use make_c_str;

mod sealed {
    pub trait Sealed {}
}

/// A trait representing mutability of the pointer.
///
/// Pointer can either be [`Const`] or [`Mut`].
///
/// ## Const pointers
/// Const pointers can only be converted to **immutable** Rust referential types.
/// There is no way to obtain a mutable reference from such pointer.
///
/// In some cases, we need to be able to mutate the data behind a shared pointer.
/// There is an example of such use case - namely [`crate::cass_types::CassDataType`].
/// argconv API does not provide a way to mutate such pointer - one can only convert the pointer
/// to [`Arc`] or &. It is the API user's responsibility to implement sound interior mutability
/// pattern in such case. This is what we currently do - CassDataType wraps CassDataTypeInner
/// inside an `UnsafeCell` to implement interior mutability for the type.
/// Other example is [`crate::future::CassFuture`] which uses Mutex.
///
/// ## Mut pointers
/// Mut pointers can be converted to both immutable and mutable Rust referential types.
pub trait Mutability: sealed::Sealed {}

/// Represents immutable pointer.
pub struct Const;
impl sealed::Sealed for Const {}
impl Mutability for Const {}

/// Represents mutable pointer.
pub struct Mut;
impl sealed::Sealed for Mut {}
impl Mutability for Mut {}

/// Represents additional properties of the pointer.
pub trait Properties: sealed::Sealed {
    type Mutability: Mutability;
}

impl<M: Mutability> sealed::Sealed for (M,) {}
impl<M: Mutability> Properties for (M,) {
    type Mutability = M;
}

/// Represents a valid non-dangling pointer.
///
/// ## Safety and validity guarantees
/// Apart from trivial constructors such as [`CassPtr::null()`] and [`CassPtr::null_mut()`], there
/// is only one way to construct a [`CassPtr`] instance - from raw pointer via [`CassPtr::from_raw()`].
/// This constructor is `unsafe`. It is user's responsibility to ensure that the raw pointer
/// provided to the constructor is **valid**. In other words, the pointer comes from some valid
/// allocation, or from some valid reference.
///
/// ## Generic lifetime and aliasing guarantees
/// We distinguish two types of pointers: immutable ([`Const`]) and mutable ([`Mut`]).
/// Immutable pointers can be converted to immutable (&) references, while mutable pointers
/// can be converted to either immutable (&) or mutable (&mut) reference. User needs to pick
/// the correct mutability property of the pointer during construction. This is yet another
/// reason why [`CassPtr::from_raw`] is `unsafe`.
///
/// Pointer is parameterized by the lifetime. Thanks to that, we can represent
/// the `Ownership` of the pointer. Once again, user is responsible for "picking"
/// the correct lifetime when creating the pointer. For example, when raw pointer
/// comes from [`Box::into_raw()`], user could create a [`CassPtr<'static, T, (Mut,)>`].
/// `'static` lifetime represents that user is the exclusive **owner** of the pointee, and
/// is responsible for freeing the memory (e.g. via [`Box::from_raw()`]).
/// On the other hand, when pointer is created from some immutable reference `&'a T`,
/// the correct choice of CassPtr would be [`CassPtr<'a, T, (Const,)>`]. It means that
/// holder of the created pointer **borrows** the pointee (with some lifetime `'a`
/// inherited from the immutable borrow `&'a T`).
///
/// Both [`CassPtr::as_ref()`] and [`CassPtr::as_mut_ref()`] consume the pointer.
/// At first glance, it seems impossible to obtain multiple immutable reference from one pointer.
/// This is why pointer reborrowing mechanism is introduced. There are two methods: [`CassPtr::borrow()`]
/// and [`CassPtr::borrow_mut()`]. Both of them cooperate with borrow checker and enforce
/// aliasing XOR mutability principle at compile time.
///
/// ## Safe conversions to referential types
/// Thanks to the above guarantees, conversions to referential types are **safe**.
/// See methods [`CassPtr::as_ref()`] and [`CassPtr::as_mut_ref()`].
///
/// ## Memory layout
/// We use repr(transparent), so the struct has the same layout as underlying [`Option<NonNull<T>>`].
/// Thanks to https://doc.rust-lang.org/std/option/#representation optimization,
/// we are guaranteed, that for `T: Sized`, our struct has the same layout
/// and function call ABI as simply [`NonNull<T>`].
#[repr(transparent)]
pub struct CassPtr<'a, T: Sized, P: Properties> {
    ptr: Option<NonNull<T>>,
    _phantom: PhantomData<&'a P>,
}

/// Owned immutable pointer.
/// Can be used for pointers with shared ownership - e.g. pointers coming from [`Arc`] allocation.
pub type CassOwnedPtr<T> = CassPtr<'static, T, (Const,)>;

/// Borrowed immutable pointer.
/// Can be used for pointers created from some immutable reference.
pub type CassBorrowedPtr<'a, T> = CassPtr<'a, T, (Const,)>;

/// Owned mutable pointer.
/// Can be used for pointers with exclusive ownership - e.g. pointers coming from [`Box`] allocation.
pub type CassOwnedMutPtr<T> = CassPtr<'static, T, (Mut,)>;

/// Borrowed mutable pointer.
/// This can be for example obtained from mutable reborrow of some [`CassOwnedMutPtr`].
pub type CassBorrowedMutPtr<'a, T> = CassPtr<'a, T, (Mut,)>;

/// Pointer constructors.
impl<T: Sized, P: Properties> CassPtr<'_, T, P> {
    fn null() -> Self {
        CassPtr {
            ptr: None,
            _phantom: PhantomData,
        }
    }

    fn is_null(&self) -> bool {
        self.ptr.is_none()
    }

    /// Constructs [`CassPtr`] from raw pointer.
    ///
    /// ## Safety
    /// User needs to ensure that the pointer is **valid**.
    /// User is also responsible for picking correct mutability property and lifetime
    /// of the created pointer. For more information, see the documentation of [`CassPtr`].
    unsafe fn from_raw(raw: *const T) -> Self {
        CassPtr {
            ptr: NonNull::new(raw as *mut T),
            _phantom: PhantomData,
        }
    }
}

/// Conversion to raw pointer.
impl<T: Sized, P: Properties> CassPtr<'_, T, P> {
    fn to_raw(&self) -> Option<*mut T> {
        self.ptr.map(|ptr| ptr.as_ptr())
    }
}

/// Constructors exclusive to mutable pointers.
impl<T: Sized> CassPtr<'_, T, (Mut,)> {
    fn null_mut() -> Self {
        CassPtr {
            ptr: None,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T: Sized, P: Properties> CassPtr<'a, T, P> {
    /// Converts a pointer to an optional valid reference.
    /// The reference inherits the lifetime of the pointer.
    #[allow(clippy::wrong_self_convention)]
    fn as_ref(self) -> Option<&'a T> {
        // SAFETY: Thanks to the validity and aliasing ^ mutability guarantees,
        // we can safely convert the pointer to valid immutable reference with
        // correct lifetime.
        unsafe { self.ptr.map(|p| p.as_ref()) }
    }
}

impl<'a, T: Sized> CassPtr<'a, T, (Mut,)> {
    /// Converts a pointer to an optional valid mutable reference.
    /// The reference inherits the lifetime of the pointer.
    #[allow(clippy::wrong_self_convention)]
    fn as_mut_ref(self) -> Option<&'a mut T> {
        // SAFETY: Thanks to the validity and aliasing ^ mutability guarantees,
        // we can safely convert the pointer to valid mutable (and exclusive) reference with
        // correct lifetime.
        unsafe { self.ptr.map(|mut p| p.as_mut()) }
    }
}

impl<T: Sized, P: Properties> CassPtr<'_, T, P> {
    /// Immutably reborrows the pointer.
    /// Resulting pointer inherits the lifetime from the immutable borrow
    /// of original pointer.
    #[allow(clippy::needless_lifetimes)]
    pub fn borrow<'a>(&'a self) -> CassPtr<'a, T, (Const,)> {
        CassPtr {
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }
}

impl<T: Sized> CassPtr<'_, T, (Mut,)> {
    /// Mutably reborrows the pointer.
    /// Resulting pointer inherits the lifetime from the mutable borrow
    /// of original pointer. Since the method accepts a mutable reference
    /// to the original pointer, we enforce aliasing ^ mutability principle at compile time.
    #[allow(clippy::needless_lifetimes)]
    pub fn borrow_mut<'a>(&'a mut self) -> CassPtr<'a, T, (Mut,)> {
        CassPtr {
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }
}

/// Defines a pointer manipulation API for non-shared heap-allocated data.
///
/// Implement this trait for types that are allocated by the driver via [`Box::new`],
/// and then returned to the user as a pointer. The user is responsible for freeing
/// the memory associated with the pointer using corresponding driver's API function.
pub trait BoxFFI {
    fn into_ptr(self: Box<Self>) -> *mut Self {
        #[allow(clippy::disallowed_methods)]
        Box::into_raw(self)
    }
    unsafe fn from_ptr(ptr: *mut Self) -> Box<Self> {
        #[allow(clippy::disallowed_methods)]
        Box::from_raw(ptr)
    }
    unsafe fn as_maybe_ref<'a>(ptr: *const Self) -> Option<&'a Self> {
        #[allow(clippy::disallowed_methods)]
        ptr.as_ref()
    }
    unsafe fn as_ref<'a>(ptr: *const Self) -> &'a Self {
        #[allow(clippy::disallowed_methods)]
        ptr.as_ref().unwrap()
    }
    unsafe fn as_mut_ref<'a>(ptr: *mut Self) -> &'a mut Self {
        #[allow(clippy::disallowed_methods)]
        ptr.as_mut().unwrap()
    }
    unsafe fn free(ptr: *mut Self) {
        std::mem::drop(BoxFFI::from_ptr(ptr));
    }
}

/// Defines a pointer manipulation API for shared heap-allocated data.
///
/// Implement this trait for types that require a shared ownership of data.
/// The data should be allocated via [`Arc::new`], and then returned to the user as a pointer.
/// The user is responsible for freeing the memory associated
/// with the pointer using corresponding driver's API function.
pub trait ArcFFI {
    fn as_ptr(self: &Arc<Self>) -> *const Self {
        #[allow(clippy::disallowed_methods)]
        Arc::as_ptr(self)
    }
    fn into_ptr(self: Arc<Self>) -> *const Self {
        #[allow(clippy::disallowed_methods)]
        Arc::into_raw(self)
    }
    unsafe fn from_ptr(ptr: *const Self) -> Arc<Self> {
        #[allow(clippy::disallowed_methods)]
        Arc::from_raw(ptr)
    }
    unsafe fn cloned_from_ptr(ptr: *const Self) -> Arc<Self> {
        #[allow(clippy::disallowed_methods)]
        Arc::increment_strong_count(ptr);
        #[allow(clippy::disallowed_methods)]
        Arc::from_raw(ptr)
    }
    unsafe fn as_maybe_ref<'a>(ptr: *const Self) -> Option<&'a Self> {
        #[allow(clippy::disallowed_methods)]
        ptr.as_ref()
    }
    unsafe fn as_ref<'a>(ptr: *const Self) -> &'a Self {
        #[allow(clippy::disallowed_methods)]
        ptr.as_ref().unwrap()
    }
    unsafe fn free(ptr: *const Self) {
        std::mem::drop(ArcFFI::from_ptr(ptr));
    }
}

/// Defines a pointer manipulation API for data owned by some other object.
///
/// Implement this trait for the types that do not need to be freed (directly) by the user.
/// The lifetime of the data is bound to some other object owning it.
///
/// For example: lifetime of CassRow is bound by the lifetime of CassResult.
/// There is no API function that frees the CassRow. It should be automatically
/// freed when user calls cass_result_free.
pub trait RefFFI {
    fn as_ptr(&self) -> *const Self {
        self as *const Self
    }
    unsafe fn as_ref<'a>(ptr: *const Self) -> &'a Self {
        #[allow(clippy::disallowed_methods)]
        ptr.as_ref().unwrap()
    }
}
