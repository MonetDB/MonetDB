# This test analyses the behaviour of OR- processing

CREATE TABLE test (id INT);
INSERT INTO test VALUES (10);
INSERT INTO test VALUES (11);
INSERT INTO test VALUES (12);

SELECT * FROM test WHERE (id=10) OR (id=11);
SELECT * FROM test WHERE ((id=10) OR (id=11)) OR id=12;
SELECT id FROM test WHERE id<11 OR id>11;
SELECT id FROM test WHERE id>11 OR id<11;
SELECT id FROM test WHERE id>=11 OR id<=10;
SELECT id FROM test WHERE id>12 OR id<10;
SELECT id FROM test WHERE id<11 OR id IS NULL;
