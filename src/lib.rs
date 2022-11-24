/*
typedef struct sqlite3_vfs sqlite3_vfs;
typedef void (*sqlite3_syscall_ptr)(void);
struct sqlite3_vfs {
  int iVersion;            /* Structure version number (currently 3) */
  int szOsFile;            /* Size of subclassed sqlite3_file */
  int mxPathname;          /* Maximum file pathname length */
  sqlite3_vfs *pNext;      /* Next registered VFS */
  const char *zName;       /* Name of this virtual file system */
  void *pAppData;          /* Pointer to application-specific data */
  int (*xOpen)(sqlite3_vfs*, sqlite3_filename zName, sqlite3_file*,
               int flags, int *pOutFlags);
  int (*xDelete)(sqlite3_vfs*, const char *zName, int syncDir);
  int (*xAccess)(sqlite3_vfs*, const char *zName, int flags, int *pResOut);
  int (*xFullPathname)(sqlite3_vfs*, const char *zName, int nOut, char *zOut);
  void *(*xDlOpen)(sqlite3_vfs*, const char *zFilename);
  void (*xDlError)(sqlite3_vfs*, int nByte, char *zErrMsg);
  void (*(*xDlSym)(sqlite3_vfs*,void*, const char *zSymbol))(void);
  void (*xDlClose)(sqlite3_vfs*, void*);
  int (*xRandomness)(sqlite3_vfs*, int nByte, char *zOut);
  int (*xSleep)(sqlite3_vfs*, int microseconds);
  int (*xCurrentTime)(sqlite3_vfs*, double*);
  int (*xGetLastError)(sqlite3_vfs*, int, char *);
  /*
  ** The methods above are in version 1 of the sqlite_vfs object
  ** definition.  Those that follow are added in version 2 or later
  */
  int (*xCurrentTimeInt64)(sqlite3_vfs*, sqlite3_int64*);
  /*
  ** The methods above are in versions 1 and 2 of the sqlite_vfs object.
  ** Those below are for version 3 and greater.
  */
  int (*xSetSystemCall)(sqlite3_vfs*, const char *zName, sqlite3_syscall_ptr);
  sqlite3_syscall_ptr (*xGetSystemCall)(sqlite3_vfs*, const char *zName);
  const char *(*xNextSystemCall)(sqlite3_vfs*, const char *zName);
  /*
  ** The methods above are in versions 1 through 3 of the sqlite_vfs object.
  ** New fields may be appended in future versions.  The iVersion
  ** value will increment whenever this happens.
  */
};
*/
#![allow(non_snake_case)]

use std::ffi::c_void;

const SQLITE_OK: i32 = 0;

#[repr(C)]
#[derive(Debug)]
pub struct sqlite3_io_methods {
    iVersion: i32,
    xClose: unsafe extern "C" fn(file: *mut sqlite3_file) -> i32,
    xRead: unsafe extern "C" fn(file: *mut sqlite3_file, buf: *mut c_void, n: i32, off: i64) -> i32,
    xWrite:
        unsafe extern "C" fn(file: *mut sqlite3_file, buf: *const c_void, n: i32, off: i64) -> i32,
    xTruncate: unsafe extern "C" fn(file: *mut sqlite3_file, size: i64) -> i32,
    xSync: unsafe extern "C" fn(file: *mut sqlite3_file, flags: i32) -> i32,
    xFileSize: unsafe extern "C" fn(file: *mut sqlite3_file, size: *mut i64) -> i32,
    xLock: unsafe extern "C" fn(file: *mut sqlite3_file, lock: i32) -> i32,
    xUnlock: unsafe extern "C" fn(file: *mut sqlite3_file, lock: i32) -> i32,
    xCheckReservedLock: unsafe extern "C" fn(file: *mut sqlite3_file, res: *mut i32) -> i32,
    xFileControl: unsafe extern "C" fn(file: *mut sqlite3_file, op: i32, arg: *mut c_void) -> i32,
    xSectorSize: unsafe extern "C" fn(file: *mut sqlite3_file) -> i32,
    xDeviceCharacteristics: unsafe extern "C" fn(file: *mut sqlite3_file) -> i32,
    // v2
    xShmMap: unsafe extern "C" fn(
        file: *mut sqlite3_file,
        pgno: i32,
        pgsize: i32,
        arg: i32,
        addr: *mut *mut c_void,
    ) -> i32,
    xShmLock: unsafe extern "C" fn(file: *mut sqlite3_file, offset: i32, n: i32, flags: i32) -> i32,
    xShmBarrier: unsafe extern "C" fn(file: *mut sqlite3_file),
    xShmUnmap: unsafe extern "C" fn(file: *mut sqlite3_file, delete_flag: i32) -> i32,
    // v3
    xFetch: unsafe extern "C" fn(
        file: *mut sqlite3_file,
        off: i64,
        n: i32,
        addr: *mut *mut c_void,
    ) -> i32,
    xUnfetch: unsafe extern "C" fn(file: *mut sqlite3_file, off: i64, addr: *mut c_void) -> i32,
}

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
    zName: *const u8,
    pData: *const c_void,
    xOpen: unsafe extern "C" fn(
        vfs: *mut sqlite3_vfs,
        name: *const u8,
        file: *mut sqlite3_file,
        flags: i32,
        out_flags: *mut i32,
    ) -> i32,
    /* TODO
        xDelete: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const u8, sync_dir: i32) -> i32,
        xAccess: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const u8, flags: i32, res: *mut i32) -> i32,
        xFullPathname: unsafe extern "C" fn(vfs: *mut sqlite3_vfs) -> i32,
        xDlOpen: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const u8),
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

#[no_mangle]
pub fn open_impl(
    vfs: *mut sqlite3_vfs,
    name: *const u8,
    file: *mut sqlite3_file,
    flags: i32,
    out_flags: *mut i32,
) -> i32 {
    tracing::info!("x_open");
    SQLITE_OK
}
