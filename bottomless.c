#include "./libsql/sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include <stdio.h>

extern void bottomless_init();
extern struct libsql_wal_methods* bottomless_methods(struct libsql_wal_methods*);

int sqlite3_bottomless_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
) {
  SQLITE_EXTENSION_INIT2(pApi);

  bottomless_init();
  struct libsql_wal_methods *orig = libsql_wal_methods_find(0);
  if (!orig) {
    return SQLITE_ERROR;
  }
  struct libsql_wal_methods *methods = bottomless_methods(orig);

  return libsql_wal_methods_register(methods);
}
