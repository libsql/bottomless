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
    xRead:
        unsafe extern "C" fn(file_ptr: *mut BottomlessFile, buf: *mut u8, n: i32, off: i64) -> i32,
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
    /* v2
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
    */
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
    xDlOpen: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8) -> *const c_void,
    xDlError: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n: i32, msg: *mut u8),
    xDlSym: unsafe extern "C" fn(
        vfs: *mut sqlite3_vfs,
        arg: *mut c_void,
        symbol: *const u8,
    ) -> unsafe extern "C" fn(),
    xDlClose: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, arg: *mut c_void),
    xRandomness: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n_bytes: i32, out: *mut u8) -> i32,
    xSleep: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, ms: i32) -> i32,
    xCurrentTime: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, time: *mut f64) -> i32,
    xGetLastError: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n: i32, buf: *mut u8) -> i32,
    xCurrentTimeInt64: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, time: *mut i64) -> i32,
}

fn native_handle(file_ptr: *mut BottomlessFile) -> *mut BottomlessFile {
    unsafe { (*file_ptr).native_file as *mut BottomlessFile }
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xClose(file_ptr: *mut BottomlessFile) -> i32 {
    debug!("closing");
    ((*(*(*file_ptr).native_file).methods).xClose)(native_handle(file_ptr))
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xRead(file_ptr: *mut BottomlessFile, buf: *mut u8, n: i32, off: i64) -> i32 {
    debug!("reading {}:{}", off, n);
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
    debug!("Writing {}:{} to {}", off, n, file.name);
    let data = std::slice::from_raw_parts(buf, n as usize);
    file.replicator.as_mut().unwrap().write(off, data);
    ((*(*(*file_ptr).native_file).methods).xWrite)(native_handle(file_ptr), buf, n, off)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xTruncate(file_ptr: *mut BottomlessFile, size: i64) -> i32 {
    debug!("truncating");
    ((*(*(*file_ptr).native_file).methods).xTruncate)(native_handle(file_ptr), size)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xSync(file_ptr: *mut BottomlessFile, flags: i32) -> i32 {
    debug!("sync");
    ((*(*(*file_ptr).native_file).methods).xSync)(native_handle(file_ptr), flags)
}

unsafe extern "C" fn xFileSize(file_ptr: *mut BottomlessFile, size: *mut i64) -> i32 {
    ((*(*(*file_ptr).native_file).methods).xFileSize)(native_handle(file_ptr), size)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xLock(file_ptr: *mut BottomlessFile, lock: i32) -> i32 {
    let file = &mut *file_ptr;
    file.lock = lock;
    debug!("Lock: {} -> {}", lockstr(file.lock), lockstr(lock));
    ((*(*(*file_ptr).native_file).methods).xLock)(native_handle(file_ptr), lock)
}

#[instrument(skip(file_ptr))]
unsafe extern "C" fn xUnlock(file_ptr: *mut BottomlessFile, lock: i32) -> i32 {
    let file = &mut *file_ptr;
    debug!("Unlock: {} -> {}", lockstr(file.lock), lockstr(lock));
    if file.lock >= SQLITE_LOCK_RESERVED && lock < SQLITE_LOCK_RESERVED {
        if let Err(e) = file.replicator.as_mut().unwrap().commit() {
            error!("Commit replication failed: {}", e);
            return SQLITE_IOERR_UNLOCK;
        }
    }
    ((*(*(*file_ptr).native_file).methods).xUnlock)(native_handle(file_ptr), lock)
}

unsafe extern "C" fn xCheckReservedLock(file_ptr: *mut BottomlessFile, res: *mut i32) -> i32 {
    ((*(*(*file_ptr).native_file).methods).xCheckReservedLock)(native_handle(file_ptr), res)
}

unsafe extern "C" fn xFileControl(file_ptr: *mut BottomlessFile, op: i32, arg: *mut c_void) -> i32 {
    ((*(*(*file_ptr).native_file).methods).xFileControl)(native_handle(file_ptr), op, arg)
}

unsafe extern "C" fn xSectorSize(file_ptr: *mut BottomlessFile) -> i32 {
    ((*(*(*file_ptr).native_file).methods).xSectorSize)(native_handle(file_ptr))
}

unsafe extern "C" fn xDeviceCharacteristics(file_ptr: *mut BottomlessFile) -> i32 {
    ((*(*(*file_ptr).native_file).methods).xDeviceCharacteristics)(native_handle(file_ptr))
}

/* v2
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
*/

const BOTTOMLESS_METHODS: sqlite3_io_methods = sqlite3_io_methods {
    iVersion: 1,
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
    /* v2
    xShmMap,
    xShmLock,
    xShmBarrier,
    xShmUnmap,
    xFetch,
    xUnfetch,
    */
};

fn get_base_vfs_ptr(vfs: *mut sqlite3_vfs) -> *mut sqlite3_vfs {
    unsafe { (*vfs).pData as *mut sqlite3_vfs }
}

#[no_mangle]
pub fn bottomless_init() {
    tracing_subscriber::fmt::init();
    debug!("bottomless module initialized");
}

// VFS Methods
#[no_mangle]
#[instrument(skip(vfs, name, file, out_flags))]
pub fn xOpen(
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
        Some(index) => name_str[index + 1..].to_string(),
        None => name_str.to_string(),
    };

    let replicator = match s3::Replicator::new(file.name.clone()) {
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

    let initial_replication_policy = std::env::var("LIBSQL_BOTTOMLESS_INITIAL_REPLICATION")
        .unwrap_or_default()
        .to_lowercase();

    if native_size > 0 {
        if initial_replication_policy == "skip" {
            warn!(
                "Database file not empty ({} pages), not bootstrapping!",
                native_size / s3::Replicator::PAGE_SIZE as i64
            );
        } else {
            let replicator = file.replicator.as_mut().unwrap();
            let bucket_empty = match replicator.is_bucket_empty() {
                Ok(empty) => empty,
                Err(e) => {
                    error!("Failed to check if the bucket is empty: {}", e);
                    return SQLITE_CANTOPEN;
                }
            };
            if !bucket_empty && initial_replication_policy != "force" {
                error!("Refusing to replicate to non-empty bucket {}. Set LIBSQL_BOTTOMLESS_INITIAL_REPLICATION=force if you wish to try anyway", replicator.bucket);
                return SQLITE_CANTOPEN;
            }
            info!(
                "Initial replication to bucket {} initiated",
                replicator.bucket
            );
            for offset in (0..native_size).step_by(s3::Replicator::PAGE_SIZE) {
                let pgno = offset / s3::Replicator::PAGE_SIZE as i64 + 1;
                info!("Replicating page {}", pgno);
                let mut page_buf = [0_u8; s3::Replicator::PAGE_SIZE];
                unsafe {
                    let rc = ((*(*file.native_file).methods).xRead)(
                        file.native_file as *mut BottomlessFile,
                        page_buf.as_mut_ptr(),
                        s3::Replicator::PAGE_SIZE as i32,
                        offset as i64,
                    );
                    debug!("\tRead status: {}", rc);
                    if rc != SQLITE_OK {
                        error!("Failed to read page {} during remote bootstrap", pgno);
                        return SQLITE_CANTOPEN;
                    }
                }
                replicator.write(offset, &page_buf);
            }
            if let Err(e) = replicator.commit() {
                error!("Initial replication failed: {}", e);
            }
        }
    } else {
        let mut next_marker = None;
        loop {
            let results = match file.replicator.as_mut().unwrap().boot(next_marker) {
                Ok(results) => results,
                Err(e) => {
                    error!("Bootstrapping from the replicator failed: {}", e);
                    return SQLITE_CANTOPEN;
                }
            };
            next_marker = results.next_marker;
            for (pgno, data) in results.pages {
                debug!("Writting page {} from the replicator locally", pgno);
                unsafe {
                    let rc = ((*(*file.native_file).methods).xWrite)(
                        file.native_file as *mut BottomlessFile,
                        data.as_ptr(),
                        s3::Replicator::PAGE_SIZE as i32,
                        pgno as i64 * s3::Replicator::PAGE_SIZE as i64,
                    );
                    debug!("\tWrite status: {}", rc);
                    if rc != SQLITE_OK {
                        error!("Failed to apply page {} during remote bootstrap", pgno);
                        return SQLITE_CANTOPEN;
                    }
                }
            }
            if next_marker.is_none() {
                break;
            }
        }
    }

    SQLITE_OK
}

#[no_mangle]
pub fn xDelete(vfs: *mut sqlite3_vfs, name: *const i8, sync_dir: i32) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xDelete)(base_vfs_ptr, name, sync_dir) }
}

#[no_mangle]
pub fn xAccess(vfs: *mut sqlite3_vfs, name: *const i8, flags: i32, res: *mut i32) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xAccess)(base_vfs_ptr, name, flags, res) }
}

#[no_mangle]
pub fn xFullPathname(vfs: *mut sqlite3_vfs, name: *const i8, n: i32, out: *mut i8) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xFullPathname)(base_vfs_ptr, name, n, out) };
    debug!("Pathname: {:?}", unsafe { CStr::from_ptr(out) });
    SQLITE_OK
}

#[no_mangle]
pub fn xDlOpen(vfs: *mut sqlite3_vfs, name: *const i8) -> *const c_void {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xDlOpen)(base_vfs_ptr, name) }
}

#[no_mangle]
pub fn xDlError(vfs: *mut sqlite3_vfs, n: i32, msg: *mut u8) {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xDlError)(base_vfs_ptr, n, msg) }
}

#[no_mangle]
pub fn xDlSym(
    vfs: *mut sqlite3_vfs,
    arg: *mut c_void,
    symbol: *const u8,
) -> unsafe extern "C" fn() {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xDlSym)(vfs, arg, symbol) }
}

#[no_mangle]
pub fn xDlClose(vfs: *mut sqlite3_vfs, arg: *mut c_void) {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xDlClose)(base_vfs_ptr, arg) }
}

#[no_mangle]
pub fn xRandomness(vfs: *mut sqlite3_vfs, n_bytes: i32, out: *mut u8) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xRandomness)(base_vfs_ptr, n_bytes, out) }
}

#[no_mangle]
pub fn xSleep(vfs: *mut sqlite3_vfs, ms: i32) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xSleep)(base_vfs_ptr, ms) }
}

#[no_mangle]
pub fn xCurrentTime(vfs: *mut sqlite3_vfs, time: *mut f64) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xCurrentTime)(base_vfs_ptr, time) }
}

#[no_mangle]
pub fn xGetLastError(vfs: *mut sqlite3_vfs, n: i32, buf: *mut u8) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xGetLastError)(base_vfs_ptr, n, buf) }
}

#[no_mangle]
pub fn xCurrentTimeInt64(vfs: *mut sqlite3_vfs, time: *mut i64) -> i32 {
    let base_vfs_ptr = get_base_vfs_ptr(vfs);
    let base_vfs: &mut sqlite3_vfs = unsafe { &mut *base_vfs_ptr };
    unsafe { (base_vfs.xCurrentTimeInt64)(base_vfs_ptr, time) }
}
