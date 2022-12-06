#!/usr/bin/env bash

export LIBSQL_BOTTOMLESS_INITIAL_REPLICATION=${LIBSQL_BOTTOMLESS_INITIAL_REPLICATION:-force}
make -C .. debug
sqlite3 < smoke_test.sql
