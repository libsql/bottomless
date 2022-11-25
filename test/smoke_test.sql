.load ../target/debug/bottomless
.open file:test.db?vfs=bottomless
CREATE TABLE IF NOT EXISTS test(id);
INSERT INTO test VALUES (42);
