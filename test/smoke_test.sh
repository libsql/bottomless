#!/usr/bin/env bash

export LIBSQL_BOTTOMLESS_INITIAL_REPLICATION=${LIBSQL_BOTTOMLESS_INITIAL_REPLICATION:-force}
make -C .. debug
/home/sarna/repo/libsql/libsql < smoke_test.sql
