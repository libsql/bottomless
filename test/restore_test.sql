.bail on
.echo on
.load ../target/debug/bottomless
.open file:test.db?wal=bottomless
PRAGMA journal_mode=wal;
.mode column
SELECT v, length(v) FROM test;
