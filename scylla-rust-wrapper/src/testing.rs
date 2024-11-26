macro_rules! assert_cass_error_eq {
    ($expr:expr, $error:expr $(,)?) => {{
        use crate::argconv::ptr_to_cstr;
        use crate::external::cass_error_desc;
        let ___x = $expr;
        assert_eq!(
            ___x,
            $error,
            "expected \"{}\", instead got \"{}\"",
            ptr_to_cstr(cass_error_desc($error)).unwrap(),
            ptr_to_cstr(cass_error_desc(___x)).unwrap()
        );
    }};
}
pub(crate) use assert_cass_error_eq;

macro_rules! assert_cass_future_error_message_eq {
    ($cass_fut:ident, $error_msg_opt:expr) => {
        use crate::argconv::ptr_to_cstr_n;
        use crate::future::cass_future_error_message;

        let mut ___message: *const c_char = ::std::ptr::null();
        let mut ___msg_len: size_t = 0;
        cass_future_error_message($cass_fut.borrow(), &mut ___message, &mut ___msg_len);
        assert_eq!(ptr_to_cstr_n(___message, ___msg_len), $error_msg_opt);
    };
}
pub(crate) use assert_cass_future_error_message_eq;
