#!/usr/bin/env bash

make -C .. debug
sqlite3 < smoke_test.sql
