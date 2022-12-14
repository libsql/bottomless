#!/usr/bin/env bash

export LIBSQL_BOTTOMLESS_ENDPOINT=http://localhost:9000
make -C .. debug
/home/sarna/repo/libsql/libsql < smoke_test.sql
