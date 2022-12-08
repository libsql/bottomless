#![allow(non_snake_case)]

mod replicator;

use std::ffi::c_void;

const SQLITE_OK: i32 = 0;
const SQLITE_CANTOPEN: i32 = 14;

#[repr(C)]
#[derive(Debug)]
pub struct sqlite3_file {
    methods: *const sqlite3_io_methods,
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

#[repr(C)]
#[derive(Debug)]
pub struct sqlite3_io_methods {
    iVersion: i32,
    xClose: unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> i32,
    xRead: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, buf: *mut u8, n: i32, off: i64) -> i32,
    xWrite:
        unsafe extern "C" fn(file_ptr: *mut sqlite3_file, buf: *const u8, n: i32, off: i64) -> i32,
    xTruncate: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, size: i64) -> i32,
    xSync: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, flags: i32) -> i32,
    xFileSize: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, size: *mut i64) -> i32,
    xLock: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, lock: i32) -> i32,
    xUnlock: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, lock: i32) -> i32,
    xCheckReservedLock: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, res: *mut i32) -> i32,
    xFileControl:
        unsafe extern "C" fn(file_ptr: *mut sqlite3_file, op: i32, arg: *mut c_void) -> i32,
    xSectorSize: unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> i32,
    xDeviceCharacteristics: unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> i32,
    /* v2
    xShmMap: unsafe extern "C" fn(
        file_ptr: *mut sqlite3_file,
        pgno: i32,
        pgsize: i32,
        arg: i32,
        addr: *mut *mut c_void,
    ) -> i32,
    xShmLock:
        unsafe extern "C" fn(file_ptr: *mut sqlite3_file, offset: i32, n: i32, flags: i32) -> i32,
    xShmBarrier: unsafe extern "C" fn(file_ptr: *mut sqlite3_file),
    xShmUnmap: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, delete_flag: i32) -> i32,
    // v3
    xFetch: unsafe extern "C" fn(
        file_ptr: *mut sqlite3_file,
        off: i64,
        n: i32,
        addr: *mut *mut c_void,
    ) -> i32,
    xUnfetch:
        unsafe extern "C" fn(file_ptr: *mut sqlite3_file, off: i64, addr: *mut c_void) -> i32,
    */
}

#[repr(C)]
pub struct Wal {
    vfs: *const sqlite3_vfs,
    db_fd: *mut sqlite3_file,
    wal_fd: *mut sqlite3_file,
    callback_value: u32,
    max_wal_size: i64,
    wi_data: i32,
    size_first_block: i32,
    ap_wi_data: *const *mut u32,
    page_size: u32,
    read_lock: i16,
    sync_flags: u8,
    exclusive_mode: u8,
    write_lock: u8,
    checkpoint_lock: u8,
    read_only: u8,
    truncate_on_commit: u8,
    sync_header: u8,
    pad_to_section_boundary: u8,
    b_shm_unreliable: u8,
    hdr: WalIndexHdr,
    min_frame: u32,
    recalculate_checksums: u32,
    wal_name: *const i8,
    n_checkpoints: u32,
    // if debug defined: log_error
    // if snapshot defined: p_snapshot
    // if setlk defined: *db
    wal_methods: *mut libsql_wal_methods,
}

// Only here for creating a Wal struct instance, we're not going to use it
#[repr(C)]
pub struct WalIndexHdr {
    version: u32,
    unused: u32,
    change: u32,
    is_init: u8,
    big_endian_checksum: u8,
    page_size: u16,
    last_valid_frame: u32,
    n_pages: u32,
    frame_checksum: [u32; 2],
    salt: [u32; 2],
    checksum: [u32; 2],
}

#[repr(C)]
pub struct libsql_wal_methods {
    xOpen: extern "C" fn(
        vfs: *const sqlite3_vfs,
        file: *mut sqlite3_file,
        wal_name: *const i8,
        no_shm_mode: i32,
        max_size: i64,
        methods: *mut libsql_wal_methods,
        wal: *mut *const Wal,
    ) -> i32,
    xClose: extern "C" fn(
        wal: *mut Wal,
        db: *mut c_void,
        sync_flags: i32,
        n_buf: i32,
        z_buf: *mut u8,
    ) -> i32,
    xLimit: extern "C" fn(wal: *mut Wal, limit: i64),
    xBeginReadTransaction: extern "C" fn(wal: *mut Wal, changed: *mut i32) -> i32,
    xEndReadTransaction: extern "C" fn(wal: *mut Wal) -> i32,
    xFindFrame: extern "C" fn(wal: *mut Wal, pgno: i32, frame: *mut i32) -> i32,
    xReadFrame: extern "C" fn(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32,
    xDbSize: extern "C" fn(wal: *mut Wal) -> i32,
    xBeginWriteTransaction: extern "C" fn(wal: *mut Wal) -> i32,
    xEndWriteTransaction: extern "C" fn(wal: *mut Wal) -> i32,
    xUndo: extern "C" fn(
        wal: *mut Wal,
        func: extern "C" fn(*mut c_void, i32) -> i32,
        ctx: *mut c_void,
    ) -> i32,
    xSavepoint: extern "C" fn(wal: *mut Wal, wal_data: *mut u32),
    xSavepointUndo: extern "C" fn(wal: *mut Wal, wal_data: *mut u32) -> i32,
    xFrames: extern "C" fn(
        wal: *mut Wal,
        page_size: u32,
        page_headers: *const PgHdr,
        size_after: i32,
        is_commit: i32,
        sync_flags: i32,
    ) -> i32,
    xCheckpoint: extern "C" fn(
        wal: *mut Wal,
        db: *mut c_void,
        emode: i32,
        busy_handler: extern "C" fn(busy_param: *mut c_void) -> i32,
        sync_flags: i32,
        n_buf: i32,
        z_buf: *mut u8,
        frames_in_wal: *mut i32,
        backfilled_frames: *mut i32,
    ) -> i32,
    xCallback: extern "C" fn(wal: *mut Wal) -> i32,
    xExclusiveMode: extern "C" fn(wal: *mut Wal) -> i32,
    xHeapMemory: extern "C" fn(wal: *mut Wal) -> i32,
    // snapshot: get, open, recover, check, unlock
    // enable_zipvfs: framesize
    xFile: extern "C" fn(wal: *mut Wal) -> *const c_void,
    xDb: extern "C" fn(wal: *mut Wal, db: *const c_void),
    xPathnameLen: extern "C" fn(orig_len: i32) -> i32,
    xGetPathname: extern "C" fn(buf: *mut u8, orig: *const u8, orig_len: i32),
    b_uses_shm: i32,
    name: *const u8,
    p_next: *const c_void,

    // User data
    underlying_methods: *const libsql_wal_methods,
    replicator: replicator::Replicator,
}

#[repr(C)]
pub struct PgHdr {
    page: *const c_void,
    data: *const c_void,
    extra: *const c_void,
    pcache: *const c_void,
    dirty: *const PgHdr,
    pager: *const c_void,
    pgno: i32,
    flags: u16,
}

pub extern "C" fn xOpen(
    vfs: *const sqlite3_vfs,
    db_file: *mut sqlite3_file,
    wal_name: *const i8,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *const Wal,
) -> i32 {
    tracing::trace!("Opening {}", unsafe {
        std::ffi::CStr::from_ptr(wal_name).to_str().unwrap()
    });
    let orig_methods = unsafe { &*(*methods).underlying_methods };
    let new_methods = unsafe { &mut *methods };

    let db_path = match unsafe { std::ffi::CStr::from_ptr(wal_name).to_str() } {
        Ok(s) => {
            if s.ends_with("-wal") {
                &s[0..s.len() - 4]
            } else {
                s
            }
        }
        Err(e) => {
            tracing::error!("Failed to parse file name: {}", e);
            return SQLITE_CANTOPEN;
        }
    };
    new_methods.replicator.register_db(db_path);

    let mut native_db_size: i64 = 0;
    unsafe {
        ((*(*db_file).methods).xFileSize)(db_file, &mut native_db_size as *mut i64);
    }
    tracing::warn!(
        "Native file size: {} ({} pages)",
        native_db_size,
        native_db_size / replicator::Replicator::PAGE_SIZE as i64
    );

    let rc = (orig_methods.xOpen)(vfs, db_file, wal_name, no_shm_mode, max_size, methods, wal);
    if rc != SQLITE_OK {
        return rc;
    }

    let mut native_wal_size: i64 = 0;
    unsafe {
        let wal_file = (*(*wal)).wal_fd;
        ((*(*wal_file).methods).xFileSize)(wal_file, &mut native_wal_size as *mut i64);
    }
    tracing::warn!(
        "Native -wal file size: {} ({} pages)",
        native_wal_size,
        native_wal_size / replicator::Replicator::PAGE_SIZE as i64
    );

    if native_db_size == 0 && native_wal_size == 0 {
        tracing::info!("Restoring data from bottomless storage");
    }

    let generation = replicator::Replicator::new_generation();
    tracing::warn!(
        "Generation {} ({:?})",
        generation,
        generation.get_timestamp()
    );

    /* TODO:
        1. -wal file present -> refuse to start, checkpoint first
        2a. Main database file not empty:
            a. Create a generation timeuuid
            b. Upload the database file to timeuuid/file
            c. Start a new backup session
        2b. Main database file empty:
            a. Get latest timeuuid
            b. Restore the main file + WAL logs, up to the marker
    */

    SQLITE_OK
}

fn get_orig_methods(wal: *mut Wal) -> &'static libsql_wal_methods {
    unsafe { &*((*(*wal).wal_methods).underlying_methods) }
}

pub extern "C" fn xClose(
    wal: *mut Wal,
    db: *mut c_void,
    sync_flags: i32,
    n_buf: i32,
    z_buf: *mut u8,
) -> i32 {
    tracing::debug!("Closing wal");
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xClose)(wal, db, sync_flags, n_buf, z_buf)
}

pub extern "C" fn xLimit(wal: *mut Wal, limit: i64) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xLimit)(wal, limit)
}

pub extern "C" fn xBeginReadTransaction(wal: *mut Wal, changed: *mut i32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xBeginReadTransaction)(wal, changed)
}

pub extern "C" fn xEndReadTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xEndReadTransaction)(wal)
}

pub extern "C" fn xFindFrame(wal: *mut Wal, pgno: i32, frame: *mut i32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xFindFrame)(wal, pgno, frame)
}

pub extern "C" fn xReadFrame(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xReadFrame)(wal, frame, n_out, p_out)
}

pub extern "C" fn xDbSize(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xDbSize)(wal)
}

pub extern "C" fn xBeginWriteTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xBeginWriteTransaction)(wal)
}

pub extern "C" fn xEndWriteTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xEndWriteTransaction)(wal)
}

pub extern "C" fn xUndo(
    wal: *mut Wal,
    func: extern "C" fn(*mut c_void, i32) -> i32,
    ctx: *mut c_void,
) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xUndo)(wal, func, ctx)
}

pub extern "C" fn xSavepoint(wal: *mut Wal, wal_data: *mut u32) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xSavepoint)(wal, wal_data)
}

pub extern "C" fn xSavepointUndo(wal: *mut Wal, wal_data: *mut u32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xSavepointUndo)(wal, wal_data)
}

#[tracing::instrument]
fn print_frames(page_headers: *const PgHdr) {
    let mut current_ptr = page_headers;
    loop {
        let current: &PgHdr = unsafe { &*current_ptr };
        tracing::trace!("page {} written to WAL", current.pgno);
        if current.dirty.is_null() {
            break;
        }
        current_ptr = current.dirty
    }
}

pub(crate) struct PageHdrIter {
    current_ptr: *const PgHdr,
    page_size: usize,
}

impl PageHdrIter {
    fn new(current_ptr: *const PgHdr, page_size: usize) -> Self {
        Self {
            current_ptr,
            page_size,
        }
    }
}

impl std::iter::Iterator for PageHdrIter {
    type Item = (i32, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_ptr.is_null() {
            return None;
        }
        let current_hdr: &PgHdr = unsafe { &*self.current_ptr };
        let raw_data =
            unsafe { std::slice::from_raw_parts(current_hdr.data as *const u8, self.page_size) };
        let item = Some((current_hdr.pgno, raw_data.to_vec()));
        self.current_ptr = current_hdr.dirty;
        item
    }
}

pub extern "C" fn xFrames(
    wal: *mut Wal,
    page_size: u32,
    page_headers: *const PgHdr,
    size_after: i32,
    is_commit: i32,
    sync_flags: i32,
) -> i32 {
    print_frames(page_headers);
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xFrames)(
        wal,
        page_size,
        page_headers,
        size_after,
        is_commit,
        sync_flags,
    )
}

pub extern "C" fn xCheckpoint(
    wal: *mut Wal,
    db: *mut c_void,
    emode: i32,
    busy_handler: extern "C" fn(busy_param: *mut c_void) -> i32,
    sync_flags: i32,
    n_buf: i32,
    z_buf: *mut u8,
    frames_in_wal: *mut i32,
    backfilled_frames: *mut i32,
) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xCheckpoint)(
        wal,
        db,
        emode,
        busy_handler,
        sync_flags,
        n_buf,
        z_buf,
        frames_in_wal,
        backfilled_frames,
    )
}

pub extern "C" fn xCallback(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xCallback)(wal)
}

pub extern "C" fn xExclusiveMode(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xExclusiveMode)(wal)
}

pub extern "C" fn xHeapMemory(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xHeapMemory)(wal)
}

pub extern "C" fn xFile(wal: *mut Wal) -> *const c_void {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xFile)(wal)
}

pub extern "C" fn xDb(wal: *mut Wal, db: *const c_void) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xDb)(wal, db)
}

pub extern "C" fn xPathnameLen(orig_len: i32) -> i32 {
    orig_len + 4
}

pub extern "C" fn xGetPathname(buf: *mut u8, orig: *const u8, orig_len: i32) {
    unsafe { std::ptr::copy(orig, buf, orig_len as usize) }
    unsafe { std::ptr::copy("-wal".as_ptr(), buf.offset(orig_len as isize), 4) }
}

#[no_mangle]
pub extern "C" fn bottomless_init() {
    tracing_subscriber::fmt::init();
    tracing::debug!("bottomless module initialized");
}

#[tracing::instrument]
#[no_mangle]
pub extern "C" fn bottomless_methods(
    underlying_methods: *const libsql_wal_methods,
) -> *const libsql_wal_methods {
    let vwal_name: *const u8 = "bottomless\0".as_ptr();
    let replicator = match replicator::Replicator::new() {
        Ok(repl) => repl,
        Err(e) => {
            tracing::error!("Failed to initialize replicator: {}", e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(libsql_wal_methods {
        xOpen,
        xClose,
        xLimit,
        xBeginReadTransaction,
        xEndReadTransaction,
        xFindFrame,
        xReadFrame,
        xDbSize,
        xBeginWriteTransaction,
        xEndWriteTransaction,
        xUndo,
        xSavepoint,
        xSavepointUndo,
        xFrames,
        xCheckpoint,
        xCallback,
        xExclusiveMode,
        xHeapMemory,
        xFile,
        xDb,
        xPathnameLen,
        xGetPathname,
        name: vwal_name,
        b_uses_shm: 0,
        p_next: std::ptr::null(),
        underlying_methods,
        replicator,
    }))
}
