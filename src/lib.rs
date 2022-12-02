#![allow(non_snake_case)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]

pub mod s3;

use std::ffi::{c_void, CStr};
use tracing::{debug, error, info, instrument, warn};

const SQLITE_OK: i32 = 0;
const SQLITE_CANTOPEN: i32 = 14;
const SQLITE_IOERR: i32 = 10;
const SQLITE_IOERR_UNLOCK: i32 = SQLITE_IOERR | 8 << 8;

const SQLITE_LOCK_NONE: i32 = 0; /* xUnlock() only */
const SQLITE_LOCK_SHARED: i32 = 1; /* xLock() or xUnlock() */
const SQLITE_LOCK_RESERVED: i32 = 2; /* xLock() only */
const SQLITE_LOCK_PENDING: i32 = 3; /* xLock() only */
const SQLITE_LOCK_EXCLUSIVE: i32 = 4; /* xLock() only */

fn lockstr(lock: i32) -> &'static str {
    match lock {
        SQLITE_LOCK_NONE => "none",
        SQLITE_LOCK_SHARED => "shared",
        SQLITE_LOCK_RESERVED => "reserved",
        SQLITE_LOCK_PENDING => "pending",
        SQLITE_LOCK_EXCLUSIVE => "exclusive",
        _ => panic!("invalid lock"),
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct sqlite3_io_methods {
    iVersion: i32,
    xClose: unsafe extern "C" fn(file_ptr: *mut BottomlessFile) -> i32,
    xRead: unsafe extern "C" fn(
        file_ptr: *mut BottomlessFile,
        buf: *mut c_void,
        n: i32,
        off: i64,
    ) -> i32,
    xWrite: unsafe extern "C" fn(
        file_ptr: *mut BottomlessFile,
        buf: *const u8,
        n: i32,
        off: i64,
    ) -> i32,
    xTruncate: unsafe extern "C" fn(file_ptr: *mut BottomlessFile, size: i64) -> i32,
    xSync: unsafe extern "C" fn(file_ptr: *mut BottomlessFile, flags: i32) -> i32,
    xFileSize: unsafe extern "C" fn(file_ptr: *mut BottomlessFile, size: *mut i64) -> i32,
    xLock: unsafe extern "C" fn(file_ptr: *mut BottomlessFile, lock: i32) -> i32,
    xUnlock: unsafe extern "C" fn(file_ptr: *mut BottomlessFile, lock: i32) -> i32,
    xCheckReservedLock: unsafe extern "C" fn(file_ptr: *mut BottomlessFile, res: *mut i32) -> i32,
    xFileControl:
        unsafe extern "C" fn(file_ptr: *mut BottomlessFile, op: i32, arg: *mut c_void) -> i32,
    xSectorSize: unsafe extern "C" fn(file_ptr: *mut BottomlessFile) -> i32,
    xDeviceCharacteristics: unsafe extern "C" fn(file_ptr: *mut BottomlessFile) -> i32,
    // v2
    xShmMap: unsafe extern "C" fn(
        file_ptr: *mut BottomlessFile,
        pgno: i32,
        pgsize: i32,
        arg: i32,
        addr: *mut *mut c_void,
    ) -> i32,
    xShmLock:
        unsafe extern "C" fn(file_ptr: *mut BottomlessFile, offset: i32, n: i32, flags: i32) -> i32,
    xShmBarrier: unsafe extern "C" fn(file_ptr: *mut BottomlessFile),
    xShmUnmap: unsafe extern "C" fn(file_ptr: *mut BottomlessFile, delete_flag: i32) -> i32,
    // v3
    xFetch: unsafe extern "C" fn(
        file_ptr: *mut BottomlessFile,
        off: i64,
        n: i32,
        addr: *mut *mut c_void,
    ) -> i32,
    xUnfetch:
        unsafe extern "C" fn(file_ptr: *mut BottomlessFile, off: i64, addr: *mut c_void) -> i32,
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
    lock: i32,
    replicator: Option<Box<s3::Replicator>>,
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

fn native_handle(file_ptr: *mut BottomlessFile) -> *mut BottomlessFile {
    unsafe { (*file_ptr).native_file as *mut BottomlessFile }
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xClose(file_ptr: *mut BottomlessFile) -> i32 {
    info!("");
    ((*(*(*file_ptr).native_file).methods).xClose)(native_handle(file_ptr))
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xRead(
    file_ptr: *mut BottomlessFile,
    buf: *mut c_void,
    n: i32,
    off: i64,
) -> i32 {
    info!("reading {}:{}", off, n);
    ((*(*(*file_ptr).native_file).methods).xRead)(native_handle(file_ptr), buf, n, off)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xWrite(
    file_ptr: *mut BottomlessFile,
    buf: *const u8,
    n: i32,
    off: i64,
) -> i32 {
    let file = &mut *file_ptr;
    info!("Writing {}:{} to {}", off, n, file.name);
    let data = std::slice::from_raw_parts(buf, n as usize);
    file.replicator.as_mut().unwrap().write(off, data);
    ((*(*(*file_ptr).native_file).methods).xWrite)(native_handle(file_ptr), buf, n, off)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xTruncate(file_ptr: *mut BottomlessFile, size: i64) -> i32 {
    info!("");
    ((*(*(*file_ptr).native_file).methods).xTruncate)(native_handle(file_ptr), size)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xSync(file_ptr: *mut BottomlessFile, flags: i32) -> i32 {
    info!("");
    ((*(*(*file_ptr).native_file).methods).xSync)(native_handle(file_ptr), flags)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xFileSize(file_ptr: *mut BottomlessFile, size: *mut i64) -> i32 {
    info!("");
    ((*(*(*file_ptr).native_file).methods).xFileSize)(native_handle(file_ptr), size)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xLock(file_ptr: *mut BottomlessFile, lock: i32) -> i32 {
    let file = &mut *file_ptr;
    file.lock = lock;
    info!("Lock: {} -> {}", lockstr(file.lock), lockstr(lock));
    ((*(*(*file_ptr).native_file).methods).xLock)(native_handle(file_ptr), lock)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xUnlock(file_ptr: *mut BottomlessFile, lock: i32) -> i32 {
    let file = &mut *file_ptr;
    info!("Unlock: {} -> {}", lockstr(file.lock), lockstr(lock));
    if file.lock >= SQLITE_LOCK_RESERVED && lock < SQLITE_LOCK_RESERVED {
        if let Err(e) = file
            .replicator
            .as_mut()
            .unwrap()
            .commit("libsql", &file.name)
        {
            error!("Commit replication failed: {}", e);
            //TODO: perhaps it's better to write locally anyway
            return SQLITE_IOERR_UNLOCK;
        }
    }
    ((*(*(*file_ptr).native_file).methods).xUnlock)(native_handle(file_ptr), lock)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xCheckReservedLock(file_ptr: *mut BottomlessFile, res: *mut i32) -> i32 {
    info!("");
    ((*(*(*file_ptr).native_file).methods).xCheckReservedLock)(native_handle(file_ptr), res)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xFileControl(file_ptr: *mut BottomlessFile, op: i32, arg: *mut c_void) -> i32 {
    info!("");
    ((*(*(*file_ptr).native_file).methods).xFileControl)(native_handle(file_ptr), op, arg)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xSectorSize(file_ptr: *mut BottomlessFile) -> i32 {
    info!("");
    ((*(*(*file_ptr).native_file).methods).xSectorSize)(native_handle(file_ptr))
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xDeviceCharacteristics(file_ptr: *mut BottomlessFile) -> i32 {
    info!("");
    ((*(*(*file_ptr).native_file).methods).xDeviceCharacteristics)(native_handle(file_ptr))
}

#[instrument]
unsafe extern "C" fn xShmMap(
    _file_ptr: *mut BottomlessFile,
    _pgno: i32,
    _pgsize: i32,
    _arg: i32,
    _addr: *mut *mut c_void,
) -> i32 {
    unimplemented!()
}

#[instrument]
unsafe extern "C" fn xShmLock(
    _file_ptr: *mut BottomlessFile,
    _offset: i32,
    _n: i32,
    _flags: i32,
) -> i32 {
    unimplemented!()
}

#[instrument]
unsafe extern "C" fn xShmBarrier(_file_ptr: *mut BottomlessFile) {
    unimplemented!()
}

#[instrument]
unsafe extern "C" fn xShmUnmap(_file_ptr: *mut BottomlessFile, _delete_flag: i32) -> i32 {
    unimplemented!()
}

#[instrument]
unsafe extern "C" fn xFetch(
    _file_ptr: *mut BottomlessFile,
    _off: i64,
    _n: i32,
    _addr: *mut *mut c_void,
) -> i32 {
    unimplemented!()
}

#[instrument]
unsafe extern "C" fn xUnfetch(
    _file_ptr: *mut BottomlessFile,
    _off: i64,
    _addr: *mut c_void,
) -> i32 {
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
    info!("base vfs: {:?}", unsafe {
        (*vfs).pData as *mut sqlite3_vfs
    });
    unsafe { (*vfs).pData as *mut sqlite3_vfs }
}

// VFS Methods
#[no_mangle]
#[instrument(skip(vfs, name, file, out_flags))]
pub fn open_impl(
    vfs: *mut sqlite3_vfs,
    name: *const i8,
    file: *mut sqlite3_file,
    flags: i32,
    out_flags: *mut i32,
) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };

    debug!("Opening {:?}", unsafe { CStr::from_ptr(name) });

    let file: &mut BottomlessFile = unsafe { &mut *(file as *mut BottomlessFile) };

    file.methods = &BOTTOMLESS_METHODS as *const sqlite3_io_methods;
    let name_str = match unsafe { CStr::from_ptr(name).to_str() } {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to parse file name: {}", e);
            return SQLITE_CANTOPEN;
        }
    };
    file.name = match name_str.rfind('/') {
        Some(index) => name_str[index..].to_string(),
        None => name_str.to_string(),
    };

    let replicator = match s3::Replicator::new() {
        Ok(r) => r,
        Err(e) => {
            error!("Replicator init failed: {}", e);
            return SQLITE_CANTOPEN;
        }
    };
    file.replicator = Some(Box::new(replicator));

    let layout = match std::alloc::Layout::from_size_align(base_vfs.szOsFile as usize, 8) {
        Ok(l) => l,
        Err(_) => return SQLITE_CANTOPEN,
    };
    file.native_file = unsafe { std::alloc::alloc(layout) as *mut sqlite3_file };
    let native_ret =
        unsafe { (base_vfs.xOpen)(base_vfs_ptr, name, file.native_file, flags, out_flags) };
    if native_ret != SQLITE_OK {
        return native_ret;
    }

    let mut native_size: i64 = 0;
    unsafe {
        xFileSize(file as *mut BottomlessFile, &mut native_size as *mut i64);
    }

    if native_size > 0 {
        warn!(
            "Database file not empty ({} pages), not bootstrapping!",
            native_size / s3::Replicator::PAGE_SIZE as i64
        );
    } else {
        let pages = match file.replicator.as_mut().unwrap().boot("libsql") {
            Ok(pages) => pages,
            Err(e) => {
                error!("Bootstrapping from the replicator failed: {}", e);
                return SQLITE_CANTOPEN;
            }
        };
        for (pgno, data) in pages {
            info!("Writting page {} from the replicator locally", pgno);
            unsafe {
                let rc = ((*(*file.native_file).methods).xWrite)(
                    file.native_file as *mut BottomlessFile,
                    data.as_ptr(),
                    s3::Replicator::PAGE_SIZE as i32,
                    pgno as i64 * s3::Replicator::PAGE_SIZE as i64,
                );
                info!("Write status: {}", rc);
                if rc != SQLITE_OK {
                    error!("Failed to apply page {} during remote bootstrap", pgno);
                    return SQLITE_CANTOPEN;
                }
            }
        }
    }

    SQLITE_OK
}

#[no_mangle]
pub fn bottomless_init() {
    tracing_subscriber::fmt::init();
    info!("init");
}

#[no_mangle]
#[instrument]
pub fn delete_impl(vfs: *mut sqlite3_vfs, name: *const i8, sync_dir: i32) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xDelete)(base_vfs_ptr, name, sync_dir) }
}

#[no_mangle]
#[instrument]
pub fn access_impl(vfs: *mut sqlite3_vfs, name: *const i8, flags: i32, res: *mut i32) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xAccess)(base_vfs_ptr, name, flags, res) }
}

#[no_mangle]
#[instrument]
pub fn full_pathname_impl(vfs: *mut sqlite3_vfs, name: *const i8, n: i32, out: *mut i8) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xFullPathname)(base_vfs_ptr, name, n, out) };
    debug!("Pathname: {:?}", unsafe { CStr::from_ptr(out) });
    SQLITE_OK
}
