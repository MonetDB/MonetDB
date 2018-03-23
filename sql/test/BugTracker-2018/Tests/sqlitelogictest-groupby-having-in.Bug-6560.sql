CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES(22,6,8), (28,57,45), (82,44,71);
SELECT + - col2 FROM tab1 AS cor0 GROUP BY col0, col2 HAVING ( col2 / + 15 + + 88 ) IN ( AVG ( col2 ) );
DROP TABLE tab1;
