.bail on
.load ../target/debug/bottomless
.open file:test.db?wal=bottomless
PRAGMA journal_mode=wal;
DROP TABLE IF EXISTS test;
CREATE TABLE test(v);
INSERT INTO test VALUES (42);
INSERT INTO test VALUES (zeroblob(8193));
INSERT INTO test VALUES ('hey');
.mode column
SELECT v, length(v) FROM test;
