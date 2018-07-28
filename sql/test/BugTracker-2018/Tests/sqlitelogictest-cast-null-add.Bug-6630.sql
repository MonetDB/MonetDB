CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);

INSERT INTO tab1 VALUES(51,14,96), (85,5,59), (91,47,68);
INSERT INTO tab2 VALUES(64,77,40), (75,67,58), (46,51,23);

SELECT CAST ( 0 + CAST ( NULL AS INTEGER ) + 0 AS BIGINT ); -- NULL

-- A single column with 3 NULL values
SELECT CAST ( 90 + CAST ( NULL AS INTEGER ) + + - 90 AS BIGINT ) FROM tab1 AS cor0 WHERE NULL IS NULL;

SELECT CAST ( 24 - CAST ( NULL AS INTEGER ) + - 29 + COUNT ( * ) + - 46 AS BIGINT ) AS col2 FROM tab2; -- NULL

SELECT DISTINCT CAST ( + 2 + + CAST ( NULL AS INTEGER ) - - ( + 69 ) AS BIGINT ) FROM tab2 AS cor0; -- NULL

SELECT ALL CAST ( - 8 + CAST ( NULL AS INTEGER ) + 43 AS BIGINT ) FROM tab2 cor0; -- A single column with 3 NULL values

-- A single column with 3 NULL values
SELECT ALL CAST ( 51 + + CAST ( NULL AS INTEGER ) - - - 17 AS BIGINT ) AS col0 FROM tab2;

-- Two columns with 3 NULL values
SELECT ALL CAST ( col1 / - - ( + CAST ( NULL AS INTEGER ) ) AS BIGINT ) col0,
           CAST ( 39 + + + CAST ( NULL AS INTEGER ) + + - 10 + col1 / - col0 AS BIGINT ) AS col1 FROM tab2;

DROP TABLE tab1;
DROP TABLE tab2;
