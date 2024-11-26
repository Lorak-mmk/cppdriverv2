use crate::argconv::ArcFFI;
use crate::argconv::CassBorrowedPtr;
use crate::argconv::CassOwnedPtr;
use crate::cass_error::CassError;
use crate::types::size_t;
use libc::{c_int, strlen};
use openssl::ssl::SslVerifyMode;
use openssl_sys::{
    BIO_free_all, BIO_new_mem_buf, EVP_PKEY_free, PEM_read_bio_PrivateKey, PEM_read_bio_X509,
    SSL_CTX_add_extra_chain_cert, SSL_CTX_free, SSL_CTX_new, SSL_CTX_set_cert_store,
    SSL_CTX_set_verify, SSL_CTX_use_PrivateKey, SSL_CTX_use_certificate, TLS_method,
    X509_STORE_add_cert, X509_STORE_new, X509_free, BIO, SSL_CTX, X509_STORE,
};
use std::convert::TryInto;
use std::os::raw::c_char;
use std::os::raw::c_void;
use std::sync::Arc;

pub struct CassSsl {
    pub(crate) ssl_context: *mut SSL_CTX,
    pub(crate) trusted_store: *mut X509_STORE,
}

impl ArcFFI for CassSsl {}

pub const CASS_SSL_VERIFY_NONE: i32 = 0x00;
pub const CASS_SSL_VERIFY_PEER_CERT: i32 = 0x01;
pub const CASS_SSL_VERIFY_PEER_IDENTITY: i32 = 0x02;
pub const CASS_SSL_VERIFY_PEER_IDENTITY_DNS: i32 = 0x04;

#[no_mangle]
pub unsafe extern "C" fn cass_ssl_new() -> CassOwnedPtr<CassSsl> {
    openssl_sys::init();
    cass_ssl_new_no_lib_init()
}

#[no_mangle]
pub unsafe extern "C" fn cass_ssl_new_no_lib_init() -> CassOwnedPtr<CassSsl> {
    let ssl_context: *mut SSL_CTX = SSL_CTX_new(TLS_method());
    let trusted_store: *mut X509_STORE = X509_STORE_new();

    SSL_CTX_set_cert_store(ssl_context, trusted_store);
    SSL_CTX_set_verify(ssl_context, CASS_SSL_VERIFY_NONE, None);

    let ssl = CassSsl {
        ssl_context,
        trusted_store,
    };

    ArcFFI::into_ptr(Arc::new(ssl))
}

// This is required for the type system to impl Send + Sync for Arc<CassSsl>.
// Otherwise, clippy complains about using Arc where Rc would do.
// In our case, though, we need to use Arc because we potentially do share
// the Arc between threads, so employing Rc here would lead to races.
unsafe impl Send for CassSsl {}
unsafe impl Sync for CassSsl {}

impl Drop for CassSsl {
    fn drop(&mut self) {
        unsafe {
            SSL_CTX_free(self.ssl_context);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_ssl_free(ssl: CassOwnedPtr<CassSsl>) {
    ArcFFI::free(ssl);
}

unsafe extern "C" fn pem_password_callback(
    buf: *mut c_char,
    size: c_int,
    _rwflag: c_int,
    u: *mut c_void,
) -> c_int {
    if u.is_null() {
        return 0;
    }

    let len = strlen(u as *const c_char);
    if len == 0 {
        return 0;
    }

    let mut to_copy = size;
    if len < to_copy.try_into().unwrap() {
        to_copy = len as c_int;
    }

    // Same as: memcpy(buf, u, to_copy);
    std::ptr::copy_nonoverlapping(u as *const c_char, buf, to_copy as usize);

    len as c_int
}

#[no_mangle]
pub unsafe extern "C" fn cass_ssl_add_trusted_cert(
    ssl: CassBorrowedPtr<CassSsl>,
    cert: *const c_char,
) -> CassError {
    if cert.is_null() {
        return CassError::CASS_ERROR_SSL_INVALID_CERT;
    }

    cass_ssl_add_trusted_cert_n(ssl, cert, strlen(cert).try_into().unwrap())
}

#[no_mangle]
pub unsafe extern "C" fn cass_ssl_add_trusted_cert_n(
    ssl: CassBorrowedPtr<CassSsl>,
    cert: *const c_char,
    cert_length: size_t,
) -> CassError {
    let ssl = ArcFFI::cloned_from_ptr(ssl).unwrap();
    let bio = BIO_new_mem_buf(cert as *const c_void, cert_length.try_into().unwrap());

    if bio.is_null() {
        return CassError::CASS_ERROR_SSL_INVALID_CERT;
    }

    let x509 = PEM_read_bio_X509(
        bio,
        std::ptr::null_mut(),
        Some(pem_password_callback),
        std::ptr::null_mut(),
    );

    BIO_free_all(bio);

    if x509.is_null() {
        return CassError::CASS_ERROR_SSL_INVALID_CERT;
    }

    X509_STORE_add_cert(ssl.trusted_store, x509);
    X509_free(x509);

    CassError::CASS_OK
}

#[no_mangle]
pub unsafe extern "C" fn cass_ssl_set_verify_flags(ssl: CassBorrowedPtr<CassSsl>, flags: i32) {
    let ssl = ArcFFI::cloned_from_ptr(ssl).unwrap();

    match flags {
        CASS_SSL_VERIFY_NONE => {
            SSL_CTX_set_verify(ssl.ssl_context, SslVerifyMode::NONE.bits(), None)
        }
        CASS_SSL_VERIFY_PEER_CERT => {
            SSL_CTX_set_verify(ssl.ssl_context, SslVerifyMode::PEER.bits(), None)
        }
        _ => {
            if flags & CASS_SSL_VERIFY_PEER_IDENTITY != 0 {
                eprintln!("The CASS_SSL_VERIFY_PEER_CERT_IDENTITY is not supported, CASS_SSL_VERIFY_PEER_CERT is set in SSL context.");
            }

            if flags & CASS_SSL_VERIFY_PEER_IDENTITY_DNS != 0 {
                eprintln!("The CASS_SSL_VERIFY_PEER_CERT_IDENTITY_DNS is not supported, CASS_SSL_VERIFY_PEER_CERT is set in SSL context.");
            }

            SSL_CTX_set_verify(ssl.ssl_context, SslVerifyMode::PEER.bits(), None);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_ssl_set_cert(
    ssl: CassBorrowedPtr<CassSsl>,
    cert: *const c_char,
) -> CassError {
    if cert.is_null() {
        return CassError::CASS_ERROR_SSL_INVALID_CERT;
    }

    cass_ssl_set_cert_n(ssl, cert, strlen(cert).try_into().unwrap())
}

#[no_mangle]
pub unsafe extern "C" fn cass_ssl_set_cert_n(
    ssl: CassBorrowedPtr<CassSsl>,
    cert: *const c_char,
    cert_length: size_t,
) -> CassError {
    let ssl = ArcFFI::cloned_from_ptr(ssl).unwrap();
    let bio = BIO_new_mem_buf(cert as *const c_void, cert_length.try_into().unwrap());

    if bio.is_null() {
        return CassError::CASS_ERROR_SSL_INVALID_CERT;
    }

    let rc = SSL_CTX_use_certificate_chain_bio(ssl.ssl_context, bio);
    BIO_free_all(bio);

    if rc == 0 {
        return CassError::CASS_ERROR_SSL_INVALID_CERT;
    }

    CassError::CASS_OK
}

#[allow(non_snake_case)]
unsafe extern "C" fn SSL_CTX_use_certificate_chain_bio(
    ssl_context: *mut SSL_CTX,
    bio: *mut BIO,
) -> c_int {
    let mut ret = 0;
    let x = PEM_read_bio_X509(
        bio,
        std::ptr::null_mut(),
        Some(pem_password_callback),
        std::ptr::null_mut(),
    );

    if x.is_null() {
        return ret;
    }

    ret = SSL_CTX_use_certificate(ssl_context, x);

    if ret != 1 {
        loop {
            let ca = PEM_read_bio_X509(
                bio,
                std::ptr::null_mut(),
                Some(pem_password_callback),
                std::ptr::null_mut(),
            );

            if ca.is_null() {
                ret = 0;
                break;
            }

            let r = SSL_CTX_add_extra_chain_cert(ssl_context, ca);
            if r == 0 {
                X509_free(ca);
                ret = 0;
                break;
            }
        }
    }

    if !x.is_null() {
        X509_free(x)
    };

    ret
}

#[no_mangle]
pub unsafe extern "C" fn cass_ssl_set_private_key(
    ssl: CassBorrowedPtr<CassSsl>,
    key: *const c_char,
    password: *mut c_char,
) -> CassError {
    if key.is_null() || password.is_null() {
        return CassError::CASS_ERROR_SSL_INVALID_PRIVATE_KEY;
    }

    cass_ssl_set_private_key_n(
        ssl,
        key,
        strlen(key).try_into().unwrap(),
        password,
        strlen(password).try_into().unwrap(),
    )
}

#[no_mangle]
pub unsafe extern "C" fn cass_ssl_set_private_key_n(
    ssl: CassBorrowedPtr<CassSsl>,
    key: *const c_char,
    key_length: size_t,
    password: *mut c_char,
    _password_length: size_t,
) -> CassError {
    let ssl = ArcFFI::cloned_from_ptr(ssl).unwrap();
    let bio = BIO_new_mem_buf(key as *const c_void, key_length.try_into().unwrap());

    if bio.is_null() {
        return CassError::CASS_ERROR_SSL_INVALID_CERT;
    }

    let pkey = PEM_read_bio_PrivateKey(
        bio,
        std::ptr::null_mut(),
        Some(pem_password_callback),
        password as *mut c_void,
    );

    BIO_free_all(bio);

    if pkey.is_null() {
        return CassError::CASS_ERROR_SSL_INVALID_PRIVATE_KEY;
    }

    SSL_CTX_use_PrivateKey(ssl.ssl_context, pkey);
    EVP_PKEY_free(pkey);

    CassError::CASS_OK
}
