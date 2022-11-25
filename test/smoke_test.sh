#!/usr/bin/env bash

RUST_LOG=trace sqlite3 < smoke_test.sql
