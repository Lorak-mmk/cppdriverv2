use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::os::raw::c_char;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub(crate) use crate::cass_uuid_types::CassUuid;

pub struct CassUuidGen {
    pub clock_seq_and_node: cass_uint64_t,
    pub last_timestamp: AtomicU64,
}

impl BoxFFI for CassUuidGen {}

// Implementation directly ported from Cpp Driver implementation:

const TIME_OFFSET_BETWEEN_UTC_AND_EPOCH: u64 = 0x01B21DD213814000; // Nanoseconds
const MIN_CLOCK_SEQ_AND_NODE: u64 = 0x8080808080808080;
const MAX_CLOCK_SEQ_AND_NODE: u64 = 0x7f7f7f7f7f7f7f7f;

fn to_milliseconds(timestamp: u64) -> u64 {
    timestamp / 10000
}

fn from_unix_timestamp(timestamp: u64) -> u64 {
    (timestamp.wrapping_mul(10000)).wrapping_add(TIME_OFFSET_BETWEEN_UTC_AND_EPOCH)
}

fn set_version(timestamp: u64, version: u8) -> u64 {
    (timestamp & 0x0FFFFFFFFFFFFFFF) | ((version as u64) << 60)
}

// Ported from UuidGen::set_clock_seq_and_node
fn rand_clock_seq_and_node(node: u64) -> u64 {
    let clock_seq: u64 = rand::random();
    let mut result: u64 = 0;
    result |= (clock_seq & 0x0000000000003FFF) << 48;
    result |= 0x8000000000000000; // RFC4122 variant
    result |= node;
    result
}

// Ported from UuidGen::monotonic_timestamp, but simplified at
// a cost of performance.
fn monotonic_timestamp(last_timestamp: &mut AtomicU64) -> u64 {
    loop {
        let now = SystemTime::now();
        let now = now.duration_since(UNIX_EPOCH).unwrap();
        let now = from_unix_timestamp(now.as_millis() as u64);

        let last = last_timestamp.load(Ordering::SeqCst);
        if last < now
            && last_timestamp
                .compare_exchange(last, now, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return now;
        }
    }
}

#[no_mangle]
pub extern "C" fn cass_uuid_version(uuid: CassUuid) -> cass_uint8_t {
    ((uuid.time_and_version >> 60) & 0x0F) as cass_uint8_t
}

#[no_mangle]
pub extern "C" fn cass_uuid_timestamp(uuid: CassUuid) -> cass_uint64_t {
    let timestamp: u64 = uuid.time_and_version & 0x0FFFFFFFFFFFFFFF;
    to_milliseconds(timestamp.wrapping_sub(TIME_OFFSET_BETWEEN_UTC_AND_EPOCH))
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_min_from_time(timestamp: cass_uint64_t, output: *mut CassUuid) {
    let uuid = CassUuid {
        time_and_version: set_version(from_unix_timestamp(timestamp), 1),
        clock_seq_and_node: MIN_CLOCK_SEQ_AND_NODE,
    };

    std::ptr::write(output, uuid);
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_max_from_time(timestamp: cass_uint64_t, output: *mut CassUuid) {
    let uuid = CassUuid {
        time_and_version: set_version(from_unix_timestamp(timestamp), 1),
        clock_seq_and_node: MAX_CLOCK_SEQ_AND_NODE,
    };

    std::ptr::write(output, uuid);
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_gen_new() -> CassExclusiveMutPtr<CassUuidGen> {
    // Inspired by C++ driver implementation in its intent.
    // The original driver tries to generate a number that
    // uniquely identifies this machine and the current process.

    // In the original driver, it generates a number
    // based on local IPs, CPU info and PID.
    let machine_id = machine_uid::get().unwrap();
    let pid = process::id();

    let mut hasher = DefaultHasher::new();
    machine_id.hash(&mut hasher);
    pid.hash(&mut hasher);

    // Masking the same way as in Cpp Driver.
    let node: u64 = (hasher.finish() & 0x0000FFFFFFFFFFFF) | 0x0000010000000000 /* Multicast bit */;

    BoxFFI::into_ptr(Box::new(CassUuidGen {
        clock_seq_and_node: rand_clock_seq_and_node(node),
        last_timestamp: AtomicU64::new(0),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_gen_new_with_node(
    node: cass_uint64_t,
) -> CassExclusiveMutPtr<CassUuidGen> {
    BoxFFI::into_ptr(Box::new(CassUuidGen {
        clock_seq_and_node: rand_clock_seq_and_node(node & 0x0000FFFFFFFFFFFF),
        last_timestamp: AtomicU64::new(0),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_gen_time(
    mut uuid_gen: CassExclusiveMutPtr<CassUuidGen>,
    output: *mut CassUuid,
) {
    let uuid_gen = BoxFFI::as_mut_ref(&mut uuid_gen).unwrap();

    let uuid = CassUuid {
        time_and_version: set_version(monotonic_timestamp(&mut uuid_gen.last_timestamp), 1),
        clock_seq_and_node: uuid_gen.clock_seq_and_node,
    };

    std::ptr::write(output, uuid);
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_gen_random(_uuid_gen: *mut CassUuidGen, output: *mut CassUuid) {
    let time_and_version: u64 = rand::random();
    let clock_seq_and_node: u64 = rand::random();

    // RFC4122 variant
    let uuid = CassUuid {
        time_and_version: set_version(time_and_version, 4),
        clock_seq_and_node: (clock_seq_and_node & 0x3FFFFFFFFFFFFFFF) | 0x8000000000000000,
    };

    std::ptr::write(output, uuid);
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_gen_from_time(
    mut uuid_gen: CassExclusiveMutPtr<CassUuidGen>,
    timestamp: cass_uint64_t,
    output: *mut CassUuid,
) {
    let uuid_gen = BoxFFI::as_mut_ref(&mut uuid_gen).unwrap();

    let uuid = CassUuid {
        time_and_version: set_version(from_unix_timestamp(timestamp), 1),
        clock_seq_and_node: uuid_gen.clock_seq_and_node,
    };

    std::ptr::write(output, uuid);
}

// Implemented ourselves:

impl From<CassUuid> for Uuid {
    fn from(uuid: CassUuid) -> Self {
        // This is a strange representation that Cpp driver
        // employs. "Recovered" and validated it empirically...
        let time_and_version_msb = uuid.time_and_version & 0xFFFFFFFF;
        let time_and_version_lsb =
            (((uuid.time_and_version & 0xFFFFFFFF00000000) >> 32) as u32).rotate_left(16) as u64;

        let msb = ((time_and_version_msb << 32) | time_and_version_lsb) as u128;
        let lsb = uuid.clock_seq_and_node as u128;

        Uuid::from_u128((msb << 64) | lsb)
    }
}

impl From<Uuid> for CassUuid {
    fn from(uuid: Uuid) -> Self {
        let u128_representation = uuid.as_u128();
        let upper_u64 = (u128_representation >> 64) as u64;
        let lower_u64 = (u128_representation & 0xFFFFFFFFFFFFFFFF) as u64;

        // This is a strange representation that Cpp driver
        // employs. "Recovered" and validated it empirically...
        let msb = ((upper_u64 & 0xFFFFFFFF) as u32).rotate_left(16) as u64;
        let lsb = (upper_u64 & 0xFFFFFFFF00000000) >> 32;

        CassUuid {
            time_and_version: (msb << 32) | lsb,
            clock_seq_and_node: lower_u64,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_string(uuid_raw: CassUuid, output: *mut c_char) {
    let uuid: Uuid = uuid_raw.into();

    let string_representation = uuid.hyphenated().to_string();
    std::ptr::copy_nonoverlapping(
        string_representation.as_ptr(),
        output as *mut u8,
        string_representation.len(),
    );

    // Null-terminate
    let null_byte = output.add(string_representation.len()) as *mut c_char;
    *null_byte = 0;
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_from_string(
    value: *const c_char,
    output: *mut CassUuid,
) -> CassError {
    cass_uuid_from_string_n(value, strlen(value), output)
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_from_string_n(
    value: *const c_char,
    value_length: size_t,
    output: *mut CassUuid,
) -> CassError {
    let value_str = ptr_to_cstr_n(value, value_length);

    match value_str {
        Some(value_str) => {
            Uuid::parse_str(value_str).map_or(CassError::CASS_ERROR_LIB_BAD_PARAMS, |parsed_uuid| {
                std::ptr::write(output, parsed_uuid.into());
                CassError::CASS_OK
            })
        }
        None => CassError::CASS_ERROR_LIB_BAD_PARAMS,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_uuid_gen_free(uuid_gen: CassExclusiveMutPtr<CassUuidGen>) {
    BoxFFI::free(uuid_gen);
}
