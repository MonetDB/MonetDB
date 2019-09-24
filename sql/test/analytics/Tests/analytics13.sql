CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

SELECT
    col1 IN (SELECT ColID FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1);

SELECT
    col1 IN (SELECT ColID + col1 FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1);

SELECT
    col1 IN (SELECT SUM(ColID + col1) FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1);

SELECT
    col3 > ALL (SELECT 1 FROM tbl_ProductSales HAVING MIN(col4) > 30)
FROM another_T
GROUP BY ROLLUP(col3, col4);

SELECT
    col1 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MIN(col2) IS NULL)
FROM another_T
GROUP BY CUBE(col1, col2);

SELECT
    SUM(col1) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2)
FROM another_T
GROUP BY CUBE(col4);

SELECT
    CASE WHEN NOT col1 NOT IN (SELECT (SELECT MAX(col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON t2.col5 = t2.col1)) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY col1;

SELECT
    t1.col1 IN (SELECT ColID FROM tbl_ProductSales GROUP BY CUBE(t1.col1, tbl_ProductSales.ColID))
FROM another_T t1
GROUP BY CUBE(col1, col2);

SELECT
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON t2.col5 = t1.col1)) THEN 1 ELSE 2 END,
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON MIN(t1.col5) = t1.col1)) THEN 1 ELSE 2 END,
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales tp LEFT JOIN another_T t2 ON tp.ColID = t1.col1 AND tp.ColID = t2.col2)) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY CUBE(t1.col1, t1.col2);

SELECT
    GROUPING(t1.col6) = ALL (SELECT 1),
    GROUPING(t1.col6) = ALL (SELECT SUM(t1.col7)),
    SUM(t1.col6) = ALL (SELECT GROUPING(t1.col7)),
    GROUPING(t1.col6) = ALL (SELECT GROUPING(t1.col7)),
    GROUPING(t1.col6) = ALL (SELECT GROUPING(t1.col7) FROM tbl_ProductSales)
FROM another_T t1
GROUP BY CUBE(t1.col6, t1.col7);

SELECT
    DISTINCT
    GROUPING(t1.col6) = ALL (SELECT 1),
    GROUPING(t1.col6) = ALL (SELECT SUM(t1.col7)),
    SUM(t1.col6) = ALL (SELECT GROUPING(t1.col7)),
    GROUPING(t1.col6) = ALL (SELECT GROUPING(t1.col7)),
    GROUPING(t1.col6) = ALL (SELECT GROUPING(t1.col7) FROM tbl_ProductSales)
FROM another_T t1
GROUP BY CUBE(t1.col6, t1.col7);

SELECT
    GROUPING(t1.col6, t1.col7) IN (SELECT SUM(t2.col2) FROM another_T t2 GROUP BY t2.col5),
    NOT 32 * GROUPING(t1.col7, t1.col6) IN (SELECT MAX(t2.col2) FROM another_T t2),
    GROUPING(t1.col6, t1.col7) NOT IN (SELECT MIN(t2.col2) FROM another_T t2 GROUP BY t1.col6),
    NOT SUM(t1.col2) * GROUPING(t1.col6, t1.col6, t1.col6, t1.col6) NOT IN (SELECT MAX(t2.col6) FROM another_T t2 GROUP BY t1.col6 HAVING t1.col7 + MIN(t2.col8) < MAX(t2.col7 - t1.col6)),
    GROUPING(t1.col6) <> ANY (SELECT t1.col7 INTERSECT SELECT t1.col6),
    GROUPING(t1.col7) = ALL (SELECT GROUPING(t1.col6) UNION ALL SELECT 10 * MIN(t1.col8))
FROM another_T t1
GROUP BY CUBE(t1.col7, t1.col6);

SELECT
    NOT col1 * col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MAX(col2) > 2),
    NOT AVG(col2) * col1 <> ANY (SELECT 20 FROM tbl_ProductSales HAVING MAX(col1) IS NULL),
    NOT EXISTS (SELECT ColID - 12 FROM tbl_ProductSales GROUP BY ColID HAVING MAX(col1) IS NOT NULL OR MIN(col1) < MIN(col2)),
    CAST (NOT col1 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) | CAST (col2 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER),
    CAST (EXISTS (SELECT MAX(col5) * MAX(col4) FROM another_T GROUP BY col5, col4) AS INTEGER) & CAST (GROUPING(col1, col5) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2) AS INTEGER)
FROM another_T
GROUP BY CUBE(col1, col2, col5);

SELECT
    DISTINCT
    NOT col1 * col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MAX(col2) > 2),
    NOT AVG(col2) * col1 <> ANY (SELECT 20 FROM tbl_ProductSales HAVING MAX(col1) IS NULL),
    NOT EXISTS (SELECT ColID - 12 FROM tbl_ProductSales GROUP BY ColID HAVING MAX(col1) IS NOT NULL OR MIN(col1) < MIN(col2)),
    CAST (NOT col1 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) | CAST (col2 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER),
    CAST (EXISTS (SELECT MAX(col5) * MAX(col4) FROM another_T GROUP BY col5, col4) AS INTEGER) & CAST (GROUPING(col1, col5) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2) AS INTEGER)
FROM another_T
GROUP BY CUBE(col1, col2, col5);

SELECT
    GROUPING(col1, col2, col5, col8),
    col1 IN (SELECT ColID + col2 FROM tbl_ProductSales),
    col1 < ANY (SELECT MAX(ColID + col2) FROM tbl_ProductSales),
    col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MIN(col8) IS NULL),
    EXISTS (SELECT col2 FROM tbl_ProductSales WHERE tbl_ProductSales.ColID = another_T.col1),
    col1 + col5 = (SELECT MIN(ColID) FROM tbl_ProductSales),
    CAST(SUM(DISTINCT CASE WHEN col5 - col8 = (SELECT MIN(ColID / col2) FROM tbl_ProductSales) THEN col2 - 5 ELSE ABS(col1) END) AS BIGINT),
    (SELECT MAX(ColID + col2) FROM tbl_ProductSales) * DENSE_RANK() OVER (PARTITION BY AVG(DISTINCT col5)),
    GROUPING(col1, col5, col8) * MIN(col8) OVER (PARTITION BY col5 ORDER BY col1 ROWS UNBOUNDED PRECEDING) evil,
    MAX(col3) / 10 + GROUPING(col1, col5, col2) * 10,
    GROUP_CONCAT(CAST(col4 AS VARCHAR(32)), '-sep-') || ' plus ' || GROUPING(col1),
    NTILE(col1) OVER (ORDER BY col8 DESC),
    col2 * NULL
FROM another_T
GROUP BY CUBE(col1, col2, col5, col8), GROUPING SETS (());

SELECT
    GROUPING(col1, col2, col3, col4, col5, col6, col7, col8), AVG(col1), CAST(SUM(col2) * 3 AS BIGINT), col3 + col4,
    CAST(MAX(col5) * MIN(col6) AS BIGINT), col7, col1 IN (SELECT ColID FROM tbl_ProductSales), col2 IN (SELECT ColID + col3 FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1, col2, col3, col4, col5, col6, col7, col8) --with 8 columns, a smallint is necessary for grouping's output
ORDER BY GROUPING(col1, col2, col3, col4, col5, col6, col7, col8);

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
