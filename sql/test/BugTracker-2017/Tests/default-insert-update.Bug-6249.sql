CREATE TABLE test1 (a INT DEFAULT -1);

INSERT INTO test1 VALUES (1);
INSERT INTO test1 VALUES (DEFAULT);
INSERT INTO test1 VALUES (2), (DEFAULT), (3), (DEFAULT), (4);

SELECT * FROM test1;

UPDATE test1 SET a = DEFAULT WHERE a = 4;

SELECT * FROM test1;

DROP TABLE test1;

CREATE TABLE test2 (a INT DEFAULT 0, b CLOB, c INT, d CLOB DEFAULT 'astring');

INSERT INTO test2 VALUES (1, 'a', 1, 'a');
INSERT INTO test2 VALUES (DEFAULT, 'a', 1, DEFAULT);
INSERT INTO test2 VALUES (2, 'b', 2, 'b'), (100, 'other', -1, DEFAULT), (3, 'c', 3, 'c'), (DEFAULT, 'd', 4, 'd');

SELECT * FROM test2;

UPDATE test2 SET d = DEFAULT, b = 'bbb' WHERE a = 1;

SELECT * FROM test2;

INSERT INTO test2 VALUES (1, 'a', DEFAULT, 'a'); --throw an error
UPDATE test2 SET b = DEFAULT; --throw an error

DROP TABLE test2;
