#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdio.h>

struct BottomlessFile {
  sqlite3_file base;        /* Subclass.  MUST BE FIRST! */
  sqlite3_file *native;
  char pad[4096];            /* Private, used in Rustv*/
};

/*
struct sqlite3_vfs {
  int iVersion;
  int szOsFile;
  int mxPathname;
  sqlite3_vfs *pNext;
  const char *zName;
  void *pAppData;
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
  int (*xCurrentTimeInt64)(sqlite3_vfs*, sqlite3_int64*);
};
*/

extern void bottomless_init();
extern int open_impl(sqlite3_vfs*, const char *zName, sqlite3_file*,
               int flags, int *pOutFlags);
extern int delete_impl(sqlite3_vfs*, const char *zName, int syncDir);
extern int access_impl(sqlite3_vfs*, const char *zName, int flags, int *pResOut);
extern int full_pathname_impl(sqlite3_vfs*, const char *zName, int nOut, char *zOut);

static sqlite3_vfs bottomless_vfs;

int sqlite3_extension_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
) {
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);

  bottomless_init();

  bottomless_vfs.iVersion = 1;
  bottomless_vfs.mxPathname = 512;
  bottomless_vfs.zName = "bottomless";
  bottomless_vfs.pNext = 0;

  sqlite3_vfs *orig = sqlite3_vfs_find(0);
  if (!orig) {
    return SQLITE_ERROR;
  }
  bottomless_vfs.pAppData = orig;
  bottomless_vfs.szOsFile = sizeof(struct BottomlessFile);

  bottomless_vfs.xOpen = open_impl;
  bottomless_vfs.xDelete = delete_impl;
  bottomless_vfs.xAccess = access_impl;
  bottomless_vfs.xFullPathname = full_pathname_impl;

  rc = sqlite3_vfs_register(&bottomless_vfs, 0);
  return rc == SQLITE_OK ? SQLITE_OK_LOAD_PERMANENTLY : rc;
}
