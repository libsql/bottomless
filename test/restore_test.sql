.bail on
.echo on
.load ../target/debug/bottomless
.open file:test.db?wal=bottomless&immutable=1
PRAGMA journal_mode=wal;
.mode column
SELECT v, length(v) FROM test;
