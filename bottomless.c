#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdio.h>

struct BottomlessFile {
  sqlite3_file base;        /* Subclass.  MUST BE FIRST! */
  sqlite3_file *native;
  char pad[4096];            /* Private, used in Rustv*/
};

extern void bottomless_init();
extern int xOpen(sqlite3_vfs*, const char *zName, sqlite3_file*,
               int flags, int *pOutFlags);
extern int xDelete(sqlite3_vfs*, const char *zName, int syncDir);
extern int xAccess(sqlite3_vfs*, const char *zName, int flags, int *pResOut);
extern int xFullPathname(sqlite3_vfs*, const char *zName, int nOut, char *zOut);
extern void *xDlOpen(sqlite3_vfs*, const char *zFilename);
extern void xDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
extern void (*(*xDlSym)(sqlite3_vfs*,void*, const char *zSymbol))(void);
extern void xDlClose(sqlite3_vfs*, void*);
extern int xRandomness(sqlite3_vfs*, int nByte, char *zOut);
extern int xSleep(sqlite3_vfs*, int microseconds);
extern int xCurrentTime(sqlite3_vfs*, double*);
extern int xGetLastError(sqlite3_vfs*, int, char *);
extern int xCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);

static sqlite3_vfs bottomless_vfs;

#define BOTTOMLESS_ASSIGN(x) (bottomless_vfs.x = x);

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

  BOTTOMLESS_ASSIGN(xOpen);
  BOTTOMLESS_ASSIGN(xDelete);
  BOTTOMLESS_ASSIGN(xAccess);
  BOTTOMLESS_ASSIGN(xFullPathname);
  BOTTOMLESS_ASSIGN(xDlOpen);
  BOTTOMLESS_ASSIGN(xDlError);
  BOTTOMLESS_ASSIGN(xDlSym);
  BOTTOMLESS_ASSIGN(xDlClose);
  BOTTOMLESS_ASSIGN(xRandomness);
  BOTTOMLESS_ASSIGN(xSleep);
  BOTTOMLESS_ASSIGN(xCurrentTime);
  BOTTOMLESS_ASSIGN(xGetLastError);
  BOTTOMLESS_ASSIGN(xCurrentTimeInt64);

  rc = sqlite3_vfs_register(&bottomless_vfs, 0);
  return rc == SQLITE_OK ? SQLITE_OK_LOAD_PERMANENTLY : rc;
}
