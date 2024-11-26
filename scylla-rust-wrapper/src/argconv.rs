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

/// A trait representing an ownership of the pointer.
pub trait Ownership: sealed::Sealed {}

/// Represents an exclusive ownership of the pointer.
/// For Box-allocated pointers.
pub struct Exclusive;
impl sealed::Sealed for Exclusive {}
impl Ownership for Exclusive {}

/// Represents a shared ownership of the pointer.
/// For Arc-allocated pointers.
pub struct Shared;
impl sealed::Sealed for Shared {}
impl Ownership for Shared {}

/// Represents a borrowed pointer.
/// For pointers created from some valid reference.
pub struct Borrowed;
impl sealed::Sealed for Borrowed {}
impl Ownership for Borrowed {}

/// A trait representing mutability of the pointer.
pub trait Mutability: sealed::Sealed {}

/// Represents immutable pointer.
pub struct Const;
impl sealed::Sealed for Const {}
impl Mutability for Const {}

/// Represents mutable pointer.
pub struct Mut;
impl sealed::Sealed for Mut {}
impl Mutability for Mut {}

/// Represents pointer properties (ownership + mutability).
pub trait Properties {
    type Ownership: Ownership;
    type Mutability: Mutability;
}

impl<O: Ownership, M: Mutability> Properties for (O, M) {
    type Ownership = O;
    type Mutability = M;
}

/// An exclusive pointer type, generic over mutability.
pub type CassExclusivePtr<T, M> = CassPtr<T, (Exclusive, M)>;

/// An exclusive const pointer. Should be used for Box-allocated objects
/// that need to be read-only.
pub type CassExclusiveConstPtr<T> = CassExclusivePtr<T, Const>;

/// An exclusive mutable pointer. Should be used for Box-allocated objects
/// that need to be mutable.
pub type CassExclusiveMutPtr<T> = CassExclusivePtr<T, Mut>;

/// A shared const pointer. Should be used for Arc-allocated objects.
///
/// Notice that this type does not provide interior mutability itself.
/// It is the responsiblity of the user of this API, to provide soundness
/// in this matter (aliasing ^ mutability).
/// Take for example [`CassDataType`](crate::cass_types::CassDataType). It
/// holds underlying data in [`UnsafeCell`](std::cell::UnsafeCell) to provide
/// interior mutability.
pub type CassSharedPtr<T> = CassPtr<T, (Shared, Const)>;

/// A borrowed const pointer. Should be used for objects that reference
/// some field of already allocated object.
///
/// When operating on the pointer of this type, we do not make any assumptions
/// about the origin of the pointer, apart from the fact that it is obtained
/// from some valid reference.
///
/// In particular, it may be a reference obtained from `Box/Arc::as_ref()`.
/// Notice, however, that we do not allow to convert such pointer back to `Box/Arc`,
/// since we do not have a guarantee that corresponding memory was allocated certain way.
/// OTOH, we can safely convert the pointer to a valid reference (assuming user did not
/// provide a pointer pointing to some garbage memory).
pub type CassBorrowedPtr<T> = CassPtr<T, (Borrowed, Const)>;

// repr(transparent), so the struct has the same layout as underlying Option<NonNull<T>>.
// Thanks to https://doc.rust-lang.org/std/option/#representation optimization,
// we are guaranteed, that for T: Sized, our struct has the same layout
// and function call ABI as simply NonNull<T>.
#[repr(transparent)]
pub struct CassPtr<T: Sized, P: Properties> {
    ptr: Option<NonNull<T>>,
    _phantom: PhantomData<P>,
}

/// Unsafe implementation of clone for tests.
///
/// In tests, we play a role as C API user. We need to be able
/// to reuse the pointer and to pass it to different API function calls.
/// API functions consume the pointer, thus we need some way to obtain
/// an owned version of the pointer again.
/// We introduce unsafe `clone` method for this reason.
#[cfg(test)]
impl<T: Sized, P: Properties> CassPtr<T, P> {
    pub unsafe fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }
}

/// Methods for any pointer kind.
///
/// Notice that there are no constructors except from null(), which is trivial.
/// Thanks to that, we can make some assumptions and guarantees based on
/// the origin of the pointer. For these, see `SAFETY` comments.
impl<T: Sized, P: Properties> CassPtr<T, P> {
    fn null() -> Self {
        CassPtr {
            ptr: None,
            _phantom: PhantomData,
        }
    }

    fn is_null(&self) -> bool {
        self.ptr.is_none()
    }

    /// Converts a pointer to an optional valid reference.
    fn as_ref(&self) -> Option<&T> {
        // SAFETY: We know that our pointers either come from a valid allocation (Box or Arc),
        // or were created based on the reference to the field of some allocated object.
        // Thus, if the pointer is non-null, we are guaranteed that it's valid.
        unsafe { self.ptr.map(|p| p.as_ref()) }
    }

    /// Converts a pointer to an optional valid reference, and consumes the pointer.
    fn into_ref<'a>(self) -> Option<&'a T> {
        // SAFETY: We know that our pointers either come from a valid allocation (Box or Arc),
        // or were created based on the reference to the field of some allocated object.
        // Thus, if the pointer is non-null, we are guaranteed that it's valid.
        unsafe { self.ptr.map(|p| p.as_ref()) }
    }

    /// Converts self to raw pointer.
    fn as_raw(&self) -> *const T {
        self.ptr
            .map(|ptr| ptr.as_ptr() as *const _)
            .unwrap_or(std::ptr::null())
    }
}

/// Methods for any exclusive pointers (no matter the mutability).
impl<T, M: Mutability> CassPtr<T, (Exclusive, M)> {
    /// Creates a pointer based on a VALID Box allocation.
    /// This is the only way to obtain such pointer.
    fn from_box(b: Box<T>) -> Self {
        #[allow(clippy::disallowed_methods)]
        let ptr = Box::into_raw(b);
        CassPtr {
            ptr: NonNull::new(ptr),
            _phantom: PhantomData,
        }
    }

    /// Converts the pointer back to the owned Box (if not null).
    fn into_box(self) -> Option<Box<T>> {
        // SAFETY: We are guaranteed that if ptr is Some, then it was obtained
        // from a valid Box allocation (see new() implementation).
        unsafe {
            self.ptr.map(|p| {
                #[allow(clippy::disallowed_methods)]
                Box::from_raw(p.as_ptr())
            })
        }
    }
}

/// Methods for exclusive mutable pointers.
impl<T> CassPtr<T, (Exclusive, Mut)> {
    fn null_mut() -> Self {
        CassPtr {
            ptr: None,
            _phantom: PhantomData,
        }
    }

    /// Converts a pointer to an optional valid, and mutable reference.
    fn as_mut_ref(&mut self) -> Option<&mut T> {
        // SAFETY: We are guaranteed that if ptr is Some, then it was obtained
        // from a valid Box allocation (see new() implementation).
        unsafe { self.ptr.map(|mut p| p.as_mut()) }
    }
}

/// Define this conversion for test purposes.
/// User receives a mutable exclusive pointer from constructor,
/// but following function calls need a const exclusive pointer.
#[cfg(test)]
impl<T> CassPtr<T, (Exclusive, Mut)> {
    pub fn into_const(self) -> CassPtr<T, (Exclusive, Const)> {
        CassPtr {
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }
}

/// Methods for Shared pointers.
impl<T: Sized> CassSharedPtr<T> {
    /// Creates a pointer based on a VALID Arc allocation.
    fn from_arc(a: Arc<T>) -> Self {
        #[allow(clippy::disallowed_methods)]
        let ptr = Arc::into_raw(a);
        CassPtr {
            ptr: NonNull::new(ptr as *mut T),
            _phantom: PhantomData,
        }
    }

    /// Creates a pointer which borrows from a VALID Arc allocation.
    fn from_ref(a: &Arc<T>) -> Self {
        #[allow(clippy::disallowed_methods)]
        let ptr = Arc::as_ptr(a);
        CassPtr {
            ptr: NonNull::new(ptr as *mut T),
            _phantom: PhantomData,
        }
    }

    /// Converts the pointer back to the Arc.
    fn into_arc(self) -> Option<Arc<T>> {
        // SAFETY: The pointer can only be obtained via new() or from_ref(),
        // both of which accept an Arc.
        // It means that the pointer comes from a valid allocation.
        unsafe {
            self.ptr.map(|p| {
                #[allow(clippy::disallowed_methods)]
                Arc::from_raw(p.as_ptr())
            })
        }
    }

    /// Converts the pointer to an Arc, by increasing its reference count.
    fn clone_arced(self) -> Option<Arc<T>> {
        // SAFETY: The pointer can only be obtained via new() or from_ref(),
        // both of which accept an Arc.
        // It means that the pointer comes from a valid allocation.
        unsafe {
            self.ptr.map(|p| {
                let ptr = p.as_ptr();
                #[allow(clippy::disallowed_methods)]
                Arc::increment_strong_count(ptr);
                #[allow(clippy::disallowed_methods)]
                Arc::from_raw(ptr)
            })
        }
    }
}

/// Methods for borrowed pointers.
impl<T: Sized> CassPtr<T, (Borrowed, Const)> {
    fn from_ref(r: &T) -> Self {
        Self {
            ptr: Some(NonNull::from(r)),
            _phantom: PhantomData,
        }
    }
}

/// Defines a pointer manipulation API for non-shared heap-allocated data.
///
/// Implement this trait for types that are allocated by the driver via [`Box::new`],
/// and then returned to the user as a pointer. The user is responsible for freeing
/// the memory associated with the pointer using corresponding driver's API function.
pub trait BoxFFI: Sized {
    fn into_ptr<M: Mutability>(self: Box<Self>) -> CassExclusivePtr<Self, M> {
        CassExclusivePtr::from_box(self)
    }
    fn from_ptr<M: Mutability>(ptr: CassExclusivePtr<Self, M>) -> Option<Box<Self>> {
        ptr.into_box()
    }
    fn as_ref<M: Mutability>(ptr: &CassExclusivePtr<Self, M>) -> Option<&Self> {
        ptr.as_ref()
    }
    fn into_ref<'a, M: Mutability>(ptr: CassExclusivePtr<Self, M>) -> Option<&'a Self> {
        ptr.into_ref()
    }
    fn as_mut_ref(ptr: &mut CassExclusiveMutPtr<Self>) -> Option<&mut Self> {
        ptr.as_mut_ref()
    }
    fn free(ptr: CassExclusiveMutPtr<Self>) {
        std::mem::drop(BoxFFI::from_ptr(ptr));
    }
    #[cfg(test)]
    fn null() -> CassExclusiveConstPtr<Self> {
        CassExclusiveConstPtr::null()
    }
    fn null_mut() -> CassExclusiveMutPtr<Self> {
        CassExclusiveMutPtr::null_mut()
    }
}

/// Defines a pointer manipulation API for shared heap-allocated data.
///
/// Implement this trait for types that require a shared ownership of data.
/// The data should be allocated via [`Arc::new`], and then returned to the user as a pointer.
/// The user is responsible for freeing the memory associated
/// with the pointer using corresponding driver's API function.
pub trait ArcFFI: Sized {
    fn as_ptr(self: &Arc<Self>) -> CassSharedPtr<Self> {
        CassSharedPtr::from_ref(self)
    }
    fn into_ptr(self: Arc<Self>) -> CassSharedPtr<Self> {
        CassSharedPtr::from_arc(self)
    }
    fn from_ptr(ptr: CassSharedPtr<Self>) -> Option<Arc<Self>> {
        ptr.into_arc()
    }
    fn cloned_from_ptr(ptr: CassSharedPtr<Self>) -> Option<Arc<Self>> {
        ptr.clone_arced()
    }
    fn as_ref(ptr: &CassSharedPtr<Self>) -> Option<&Self> {
        ptr.as_ref()
    }
    fn into_ref<'a>(ptr: CassSharedPtr<Self>) -> Option<&'a Self> {
        ptr.into_ref()
    }
    fn to_raw(ptr: &CassSharedPtr<Self>) -> *const Self {
        ptr.as_raw()
    }
    fn free(ptr: CassSharedPtr<Self>) {
        std::mem::drop(ArcFFI::from_ptr(ptr));
    }
    fn null() -> CassSharedPtr<Self> {
        CassSharedPtr::null()
    }
    fn is_null(ptr: &CassSharedPtr<Self>) -> bool {
        ptr.is_null()
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
pub trait RefFFI: Sized {
    fn as_ptr(&self) -> CassBorrowedPtr<Self> {
        CassBorrowedPtr::from_ref(self)
    }
    fn as_ref(ptr: &CassBorrowedPtr<Self>) -> Option<&Self> {
        ptr.as_ref()
    }
    fn into_ref<'a>(ptr: CassBorrowedPtr<Self>) -> Option<&'a Self> {
        ptr.into_ref()
    }
    fn null() -> CassBorrowedPtr<Self> {
        CassBorrowedPtr::null()
    }
    fn is_null(ptr: &CassBorrowedPtr<Self>) -> bool {
        ptr.is_null()
    }
}
