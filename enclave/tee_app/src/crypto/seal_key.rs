use anyhow::Result;
use libc::{c_int, O_RDONLY};
use sgx_types::{
    sgx_attributes_t, sgx_key_128bit_t, sgx_key_id_t, sgx_key_request_t,
    sgx_report_data_t, sgx_report_t, sgx_target_info_t, SGX_KEYID_SIZE, SGX_KEYPOLICY_MRENCLAVE,
    SGX_KEYSELECT_SEAL, SGX_KEY_REQUEST_RESERVED2_BYTES, TSEAL_DEFAULT_FLAGSMASK,
    TSEAL_DEFAULT_MISCMASK,
};

use std::ffi::CString;

const SGXIOC_GET_KEY: u64 = 0xc010730b; // #define SGXIOC_GET_KEY _IOWR('s', 11, sgxioc_get_key_arg_t)
const SGXIOC_CREATE_REPORT: u64 = 0xc0187304; // #define SGXIOC_CREATE_REPORT _IOWR('s', 4, sgxioc_create_report_arg_t)

cfg_if::cfg_if! {
    if #[cfg(target_env = "musl")] {
        const IOCTL_GET_KEY: i32 = SGXIOC_GET_KEY as i32;
        const IOCTL_CREATE_REPORT: i32 = SGXIOC_CREATE_REPORT as i32;
    } else {
        const IOCTL_GET_KEY: u64 = SGXIOC_GET_KEY;
        const IOCTL_CREATE_REPORT: u64 = SGXIOC_CREATE_REPORT;
    }
}

// Copy from occlum/src/libos/src/fs/dev_fs/dev_sgx/mod.rs
#[repr(C)]
pub struct IoctlGetKeyArg {
    key_request: *const sgx_key_request_t, // Input
    key: *mut sgx_key_128bit_t,            // Output
}

// Copy from occlum/src/libos/src/fs/dev_fs/dev_sgx/mod.rs
#[repr(C)]
struct IoctlCreateReportArg {
    target_info: *const sgx_target_info_t, // Input (optional)
    report_data: *const sgx_report_data_t, // Input (optional)
    report: *mut sgx_report_t,             // Output
}

pub struct GetKey {
    fd: c_int,
}

impl GetKey {
    pub fn new() -> Self {
        let path = CString::new("/dev/sgx").unwrap();
        let fd = unsafe { libc::open(path.as_ptr(), O_RDONLY) };
        if fd > 0 {
            Self { fd: fd }
        } else {
            panic!("Open /dev/sgx failed")
        }
    }

    pub fn get_key(&mut self, request: *const sgx_key_request_t) -> Result<sgx_key_128bit_t> {
        let mut key: sgx_key_128bit_t = [0u8; 16];

        let key_args: IoctlGetKeyArg = IoctlGetKeyArg {
            key_request: request,
            key: &mut key,
        };

        let ret = unsafe { libc::ioctl(self.fd, IOCTL_GET_KEY, &key_args) };
        if ret < 0 {
            Err(anyhow::anyhow!("IOCTRL IOCTL_GET_KEY failed"))
        } else {
            Ok(key)
        }
    }

    pub fn create_report(&mut self, report: *mut sgx_report_t) -> Result<()> {
        let report_args: IoctlCreateReportArg = IoctlCreateReportArg {
            target_info: std::ptr::null(),
            report_data: std::ptr::null(),
            report: report,
        };

        let ret = unsafe { libc::ioctl(self.fd, IOCTL_CREATE_REPORT, &report_args) };
        if ret < 0 {
            return Err(anyhow::anyhow!("IOCTRL IOCTL_CREATE_REPORT failed"));
        }
        Ok(())
    }
}

pub fn get_key() -> [u8; 16] {
    let mut get_key = GetKey::new();

    let mut report = unsafe { std::mem::zeroed::<sgx_report_t>() };
    get_key.create_report(&mut report).unwrap();

    let attribute_mask = sgx_attributes_t {
        flags: TSEAL_DEFAULT_FLAGSMASK,
        xfrm: 0,
    };
    let key_id = sgx_key_id_t {
        id: [0u8; SGX_KEYID_SIZE],
    };
    let key_policy: u16 = SGX_KEYPOLICY_MRENCLAVE;
    let mut key_request = sgx_key_request_t {
        key_name: SGX_KEYSELECT_SEAL,
        key_policy,
        isv_svn: 0u16,
        reserved1: 0u16,
        cpu_svn: report.body.cpu_svn,
        attribute_mask,
        key_id,
        misc_mask: TSEAL_DEFAULT_MISCMASK,
        config_svn: 0u16,
        reserved2: [0u8; SGX_KEY_REQUEST_RESERVED2_BYTES],
    };

    let key = get_key.get_key(&mut key_request).unwrap();
    key
}
