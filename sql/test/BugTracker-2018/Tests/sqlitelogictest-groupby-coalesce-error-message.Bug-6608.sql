CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);

INSERT INTO tab0 VALUES (83,0,38), (26,0,79), (43,81,24);
INSERT INTO tab1 VALUES (22,6,8), (28,57,45), (82,44,71);

-- query without groupby (works ok)
SELECT DISTINCT - COALESCE ( - 86, + cor0.col1, cor0.col1, - cor0.col0 ) AS col2 FROM tab0 AS cor0;
-- query with groupby giving error, but error msg "no such aggregate 'sql_neg'" is not useful
SELECT DISTINCT - COALESCE ( - 86, + cor0.col1, cor0.col1, - cor0.col0 ) AS col2 FROM tab0 AS cor0 GROUP BY cor0.col2, cor0.col0;
-- instead it should give error as reported by next query:
SELECT COALESCE ( - 86, + cor0.col1, cor0.col1, - cor0.col0 ) AS col2 FROM tab0 AS cor0 GROUP BY cor0.col2, cor0.col0;

SELECT ALL + 33 * - COALESCE ( - 86, tab1.col2 ) + + col1 FROM tab1;
SELECT ALL + 33 * - COALESCE ( - 86, tab1.col2 ) + + col1 FROM tab1 GROUP BY tab1.col1;
SELECT COALESCE ( - 86, tab1.col2 ) FROM tab1 GROUP BY tab1.col1;

SELECT ALL CAST( + COALESCE ( - cor0.col1, cor0.col1, 63, - cor0.col2 ) * - cor0.col1 AS BIGINT) AS col1 FROM tab0 cor0;
SELECT ALL + COALESCE ( - cor0.col1, cor0.col1, 63, - cor0.col2 ) * - cor0.col1 AS col1 FROM tab0 cor0 GROUP BY cor0.col0, col1;
SELECT ALL + COALESCE ( - cor0.col1, cor0.col1, 63, cor0.col2 ) AS col1 FROM tab0 cor0 GROUP BY cor0.col0, col1;

SELECT CAST(- 38 + - tab1.col1 - tab1.col1 / COALESCE ( + 20, - tab1.col0 ) AS BIGINT) FROM tab1;
SELECT - 38 + - tab1.col1 - tab1.col1 / COALESCE ( + 20, - tab1.col0 ) FROM tab1 GROUP BY tab1.col1;
SELECT COALESCE ( + 20, tab1.col0 ) FROM tab1 GROUP BY tab1.col1;

SELECT DISTINCT COALESCE ( - 82, - cor0.col0, - CAST ( NULL AS INTEGER ) ) / - 70 FROM tab0 AS cor0;
SELECT DISTINCT COALESCE ( - 82, - cor0.col0, - CAST ( NULL AS INTEGER ) ) / - 70 FROM tab0 AS cor0 GROUP BY cor0.col2;
SELECT DISTINCT COALESCE ( - 82, cor0.col0, - CAST ( NULL AS INTEGER ) ) FROM tab0 AS cor0 GROUP BY cor0.col2;

DROP TABLE tab0;
DROP TABLE tab1;
