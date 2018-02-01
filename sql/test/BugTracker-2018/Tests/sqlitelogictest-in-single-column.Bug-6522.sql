CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab2 VALUES (64,77,40), (75,67,58), (46,51,23);
SELECT ALL * FROM tab2 WHERE + col0 IN ( - 12, col0, - col1, col1 / + col0, col1 );
SELECT ALL * FROM tab2 WHERE + col0 IN ( col0, - col1, col1 / + col0, col1 );
SELECT ALL * FROM tab2 WHERE + col0 IN ( - col1, col1 / + col0, col1 );
SELECT ALL * FROM tab2 WHERE + col0 IN ( col1 / + col0, col1 );
SELECT ALL * FROM tab2 WHERE + col0 IN ( col1 );
DROP TABLE tab2;
