#![allow(non_snake_case)]

pub mod s3;

use std::ffi::c_void;
use std::sync::Arc;
use tracing::{instrument, trace};

const SQLITE_OK: i32 = 0;
const SQLITE_CANTOPEN: i32 = 14;

#[repr(C)]
#[derive(Debug)]
pub struct sqlite3_io_methods {
    iVersion: i32,
    xClose: unsafe extern "C" fn(file: *mut BottomlessFile) -> i32,
    xRead:
        unsafe extern "C" fn(file: *mut BottomlessFile, buf: *mut c_void, n: i32, off: i64) -> i32,
    xWrite:
        unsafe extern "C" fn(file: *mut BottomlessFile, buf: *const u8, n: i32, off: i64) -> i32,
    xTruncate: unsafe extern "C" fn(file: *mut BottomlessFile, size: i64) -> i32,
    xSync: unsafe extern "C" fn(file: *mut BottomlessFile, flags: i32) -> i32,
    xFileSize: unsafe extern "C" fn(file: *mut BottomlessFile, size: *mut i64) -> i32,
    xLock: unsafe extern "C" fn(file: *mut BottomlessFile, lock: i32) -> i32,
    xUnlock: unsafe extern "C" fn(file: *mut BottomlessFile, lock: i32) -> i32,
    xCheckReservedLock: unsafe extern "C" fn(file: *mut BottomlessFile, res: *mut i32) -> i32,
    xFileControl: unsafe extern "C" fn(file: *mut BottomlessFile, op: i32, arg: *mut c_void) -> i32,
    xSectorSize: unsafe extern "C" fn(file: *mut BottomlessFile) -> i32,
    xDeviceCharacteristics: unsafe extern "C" fn(file: *mut BottomlessFile) -> i32,
    // v2
    xShmMap: unsafe extern "C" fn(
        file: *mut BottomlessFile,
        pgno: i32,
        pgsize: i32,
        arg: i32,
        addr: *mut *mut c_void,
    ) -> i32,
    xShmLock:
        unsafe extern "C" fn(file: *mut BottomlessFile, offset: i32, n: i32, flags: i32) -> i32,
    xShmBarrier: unsafe extern "C" fn(file: *mut BottomlessFile),
    xShmUnmap: unsafe extern "C" fn(file: *mut BottomlessFile, delete_flag: i32) -> i32,
    // v3
    xFetch: unsafe extern "C" fn(
        file: *mut BottomlessFile,
        off: i64,
        n: i32,
        addr: *mut *mut c_void,
    ) -> i32,
    xUnfetch: unsafe extern "C" fn(file: *mut BottomlessFile, off: i64, addr: *mut c_void) -> i32,
}

#[repr(C)]
#[derive(Debug)]
pub struct sqlite3_file {
    methods: *const sqlite3_io_methods,
}

#[repr(C)]
#[derive(Debug)]
pub struct BottomlessFile {
    methods: *const sqlite3_io_methods,
    native_file: *mut sqlite3_file,
    name: String,
    replicator: Option<Arc<s3::Replicator>>,
}

#[repr(C)]
#[derive(Debug)]
pub struct sqlite3_vfs {
    iVersion: i32,
    szOsFile: i32,
    mxPathname: i32,
    pNext: *mut sqlite3_vfs,
    zname: *const i8,
    pData: *const c_void,
    xOpen: unsafe extern "C" fn(
        vfs: *mut sqlite3_vfs,
        name: *const i8,
        file: *mut sqlite3_file,
        flags: i32,
        out_flags: *mut i32,
    ) -> i32,
    xDelete: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8, sync_dir: i32) -> i32,
    xAccess: unsafe extern "C" fn(
        vfs: *mut sqlite3_vfs,
        name: *const i8,
        flags: i32,
        res: *mut i32,
    ) -> i32,
    xFullPathname:
        unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8, n: i32, out: *mut i8) -> i32,
    /* TODO:
        xDlOpen: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8),
        xDlError: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n: i32, msg: *mut u8),
        xDlSym: unsafe extern "C" fn() -> unsafe extern "C" fn(vfs: *mut sqlite3_vfs, arg: *mut c_void, symbol: *const u8),
        xDlClose: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, arg: *mut c_void),
        xRandomness: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n_bytes: i32, out: *mut u8) -> i32,
        xSleep: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, ms: i32) -> i32,
        xCurrentTime: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, time: *mut f64) -> i32,
        xGetLastError: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n: i32, buf: *mut u8) -> i32,
        // v2
        xCurrentTimeInt64: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, time: *mut i64) -> i32,
        // v3
        // system calls, ignore for now
    */
}

fn native_handle(file: *mut BottomlessFile) -> *mut BottomlessFile {
    unsafe { (*file).native_file as *mut BottomlessFile }
}

#[instrument]
unsafe extern "C" fn xClose(file: *mut BottomlessFile) -> i32 {
    trace!("");
    ((*(*(*file).native_file).methods).xClose)(native_handle(file))
}

#[instrument]
unsafe extern "C" fn xRead(file: *mut BottomlessFile, buf: *mut c_void, n: i32, off: i64) -> i32 {
    trace!("reading {}:{}", off, n);
    ((*(*(*file).native_file).methods).xRead)(native_handle(file), buf, n, off)
}

#[instrument]
unsafe extern "C" fn xWrite(
    file_ptr: *mut BottomlessFile,
    buf: *const u8,
    n: i32,
    off: i64,
) -> i32 {
    trace!("writing {}:{}", off, n);
    let file = &mut *file_ptr;
    let data = std::slice::from_raw_parts(buf, n as usize);
    file.replicator.as_ref().unwrap().send(
        /*fixme: derived from db name*/ "libsql",
        off as usize,
        data,
    );
    ((*(*(*file).native_file).methods).xWrite)(native_handle(file_ptr), buf, n, off)
}

#[instrument]
unsafe extern "C" fn xTruncate(file: *mut BottomlessFile, size: i64) -> i32 {
    trace!("");
    ((*(*(*file).native_file).methods).xTruncate)(native_handle(file), size)
}

#[instrument]
unsafe extern "C" fn xSync(file: *mut BottomlessFile, flags: i32) -> i32 {
    trace!("");
    ((*(*(*file).native_file).methods).xSync)(native_handle(file), flags)
}

#[instrument]
unsafe extern "C" fn xFileSize(file: *mut BottomlessFile, size: *mut i64) -> i32 {
    trace!("");
    ((*(*(*file).native_file).methods).xFileSize)(native_handle(file), size)
}

#[instrument]
unsafe extern "C" fn xLock(file: *mut BottomlessFile, lock: i32) -> i32 {
    trace!("");
    ((*(*(*file).native_file).methods).xLock)(native_handle(file), lock)
}

#[instrument]
unsafe extern "C" fn xUnlock(file: *mut BottomlessFile, lock: i32) -> i32 {
    trace!("");
    ((*(*(*file).native_file).methods).xUnlock)(native_handle(file), lock)
}

#[instrument]
unsafe extern "C" fn xCheckReservedLock(file: *mut BottomlessFile, res: *mut i32) -> i32 {
    trace!("");
    ((*(*(*file).native_file).methods).xCheckReservedLock)(native_handle(file), res)
}

#[instrument]
unsafe extern "C" fn xFileControl(file: *mut BottomlessFile, op: i32, arg: *mut c_void) -> i32 {
    trace!("");
    ((*(*(*file).native_file).methods).xFileControl)(native_handle(file), op, arg)
}

#[instrument]
unsafe extern "C" fn xSectorSize(file: *mut BottomlessFile) -> i32 {
    trace!("");
    ((*(*(*file).native_file).methods).xSectorSize)(native_handle(file))
}

#[instrument]
unsafe extern "C" fn xDeviceCharacteristics(file: *mut BottomlessFile) -> i32 {
    trace!("");
    ((*(*(*file).native_file).methods).xDeviceCharacteristics)(native_handle(file))
}

#[instrument]
unsafe extern "C" fn xShmMap(
    _file: *mut BottomlessFile,
    _pgno: i32,
    _pgsize: i32,
    _arg: i32,
    _addr: *mut *mut c_void,
) -> i32 {
    unimplemented!()
}

#[instrument]
unsafe extern "C" fn xShmLock(
    _file: *mut BottomlessFile,
    _offset: i32,
    _n: i32,
    _flags: i32,
) -> i32 {
    unimplemented!()
}

#[instrument]
unsafe extern "C" fn xShmBarrier(_file: *mut BottomlessFile) {
    unimplemented!()
}

#[instrument]
unsafe extern "C" fn xShmUnmap(_file: *mut BottomlessFile, _delete_flag: i32) -> i32 {
    unimplemented!()
}

unsafe extern "C" fn xFetch(
    _file: *mut BottomlessFile,
    _off: i64,
    _n: i32,
    _addr: *mut *mut c_void,
) -> i32 {
    unimplemented!()
}
unsafe extern "C" fn xUnfetch(_file: *mut BottomlessFile, _off: i64, _addr: *mut c_void) -> i32 {
    unimplemented!()
}

const BOTTOMLESS_METHODS: sqlite3_io_methods = sqlite3_io_methods {
    iVersion: 2,
    xClose,
    xRead,
    xWrite,
    xTruncate,
    xSync,
    xFileSize,
    xLock,
    xUnlock,
    xCheckReservedLock,
    xFileControl,
    xSectorSize,
    xDeviceCharacteristics,
    xShmMap,
    xShmLock,
    xShmBarrier,
    xShmUnmap,
    xFetch,
    xUnfetch,
};

fn get_base_vfs_ptr(vfs: *mut sqlite3_vfs) -> *mut sqlite3_vfs {
    trace!("base vfs: {:?}", unsafe {
        (*vfs).pData as *mut sqlite3_vfs
    });
    unsafe { (*vfs).pData as *mut sqlite3_vfs }
}

fn cstr(input: *const i8) -> String {
    unsafe {
        std::ffi::CStr::from_ptr(input)
            .to_str()
            .unwrap()
            .to_string()
    }
}

// VFS Methods
#[no_mangle]
#[instrument]
pub fn open_impl(
    vfs: *mut sqlite3_vfs,
    name: *const i8,
    file: *mut sqlite3_file,
    flags: i32,
    out_flags: *mut i32,
) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };

    trace!("Name: <{}>", cstr(name));

    let file: &mut BottomlessFile = unsafe { &mut *(file as *mut BottomlessFile) };

    file.methods = &BOTTOMLESS_METHODS as *const sqlite3_io_methods;
    file.name = cstr(name);
    let replicator = s3::Replicator::new();
    file.replicator = Some(Arc::new(replicator));

    let layout = match std::alloc::Layout::from_size_align(base_vfs.szOsFile as usize, 8) {
        Ok(l) => l,
        Err(_) => return SQLITE_CANTOPEN,
    };
    trace!("Layout: {:?}", layout);
    file.native_file = unsafe { std::alloc::alloc(layout) as *mut sqlite3_file };
    let native_ret =
        unsafe { ((*base_vfs).xOpen)(base_vfs_ptr, name, file.native_file, flags, out_flags) };
    trace!("Native file open: {}", native_ret);
    if native_ret != SQLITE_OK {
        return native_ret;
    }
    trace!("ok");
    SQLITE_OK
}

#[no_mangle]
pub fn bottomless_init() {
    tracing_subscriber::fmt::init();
    trace!("init");
}

#[no_mangle]
#[instrument]
pub fn delete_impl(vfs: *mut sqlite3_vfs, name: *const i8, sync_dir: i32) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { ((*base_vfs).xDelete)(base_vfs_ptr, name, sync_dir) }
}

#[no_mangle]
#[instrument]
pub fn access_impl(vfs: *mut sqlite3_vfs, name: *const i8, flags: i32, res: *mut i32) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { ((*base_vfs).xAccess)(base_vfs_ptr, name, flags, res) }
}

#[no_mangle]
#[instrument]
pub fn full_pathname_impl(vfs: *mut sqlite3_vfs, name: *const i8, n: i32, out: *mut i8) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { ((*base_vfs).xFullPathname)(base_vfs_ptr, name, n, out) };
    trace!("Pathname: {}", cstr(out));
    SQLITE_OK
}
