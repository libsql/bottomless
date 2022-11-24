#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdio.h>

extern int open_impl(sqlite3_vfs*, const char *zName, sqlite3_file*,
               int flags, int *pOutFlags);

static sqlite3_vfs bottomless_vfs;

int sqlite3_extension_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
) {
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);

  bottomless_vfs.iVersion = 1;
  bottomless_vfs.szOsFile = 32;
  bottomless_vfs.mxPathname = 32;
  bottomless_vfs.zName = "bottomless";
  bottomless_vfs.pNext = 0;
  bottomless_vfs.xOpen = open_impl;

  rc = sqlite3_vfs_register(&bottomless_vfs, 0);
  return rc == SQLITE_OK ? SQLITE_OK_LOAD_PERMANENTLY : rc;
}
