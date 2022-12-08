#![allow(non_snake_case)]

use std::ffi::c_void;

#[repr(C)]
pub struct Wal {
    vfs: *const c_void,
    db_fd: *const c_void,
    wal_fd: *const c_void,
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
    wal_name: *const u8,
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
        vfs: *const c_void,
        file: *const c_void,
        wal_name: *const u8,
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
    vfs: *const c_void,
    file: *const c_void,
    wal_name: *const u8,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *const Wal,
) -> i32 {
    tracing::trace!("Opening {}", unsafe {
        std::ffi::CStr::from_ptr(wal_name as *const i8)
            .to_str()
            .unwrap()
    });
    let orig_methods = unsafe { &*(*methods).underlying_methods };
    (orig_methods.xOpen)(vfs, file, wal_name, no_shm_mode, max_size, methods, wal)
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
    orig_len
}

pub extern "C" fn xGetPathname(buf: *mut u8, orig: *const u8, orig_len: i32) {
    unsafe { std::ptr::copy(orig, buf, orig_len as usize) }
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
    }))
}
