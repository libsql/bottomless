.bail on
.echo on
.load ../target/debug/bottomless
.open file:test.db?wal=bottomless
PRAGMA journal_mode=wal;
DROP TABLE IF EXISTS test;
CREATE TABLE test(v);
INSERT INTO test VALUES (42);
INSERT INTO test VALUES (zeroblob(8193));
INSERT INTO test VALUES ('hey');
.mode column

BEGIN;
INSERT INTO test VALUES ('presavepoint');
SAVEPOINT test1;
INSERT INTO test VALUES (43);
INSERT INTO test VALUES (zeroblob(12553));
INSERT INTO test VALUES ('heyyyy');
ROLLBACK TO SAVEPOINT test1;
COMMIT;

SELECT v, length(v) FROM test;
.exit
