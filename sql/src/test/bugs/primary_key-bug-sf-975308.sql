CREATE TABLE test (
y INTEGER,
x INTEGER,
PRIMARY KEY(x, y)
);

INSERT INTO test values( 2,3 );
INSERT INTO test values( 2,4 );
SELECT * from test;
COMMIT;

INSERT INTO test values( 2,4 );
ROLLBACK;
SELECT * from test;

UPDATE test SET y = 1;
SELECT * from test;
COMMIT;

UPDATE test SET x = 1;
SELECT * from test;
ROLLBACK;
SELECT * from test;

DROP table test;
COMMIT;
