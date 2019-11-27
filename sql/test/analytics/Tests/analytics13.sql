CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

SELECT
    (SELECT GROUPING(colID) FROM tbl_ProductSales)
FROM another_T t1; --Postgresql doesn't support this one

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
    1 IN (SELECT 1 FROM tbl_ProductSales GROUP BY CUBE(t1.col1, tbl_ProductSales.ColID))
FROM another_T t1;

SELECT
    t1.col1 IN (SELECT ColID FROM tbl_ProductSales GROUP BY CUBE(t1.col1, tbl_ProductSales.ColID))
FROM another_T t1
GROUP BY CUBE(col1, col2);

SELECT
    NOT GROUPING(t1.col6) IN (SELECT SUM(t1.col6) FROM tbl_ProductSales tp HAVING MAX(t1.col1) > MIN(tp.colID)),
    GROUPING(t1.col6) IN (SELECT SUM(t1.col7) HAVING GROUPING(t1.col7) < SUM(t1.col4)),
    GROUPING(t1.col6) = ALL (SELECT 1),
    GROUPING(t1.col6) = ALL (SELECT SUM(t1.col7)),
    SUM(t1.col6) = ALL (SELECT GROUPING(t1.col7)),
    GROUPING(t1.col6) = ALL (SELECT GROUPING(t1.col7)),
    GROUPING(t1.col6) = ALL (SELECT GROUPING(t1.col7) FROM tbl_ProductSales)
FROM another_T t1
GROUP BY CUBE(t1.col6, t1.col7);

SELECT
    DISTINCT
    NOT GROUPING(t1.col6) IN (SELECT SUM(t1.col6) FROM tbl_ProductSales tp HAVING MAX(t1.col1) > MIN(tp.colID)),
    GROUPING(t1.col6) IN (SELECT SUM(t1.col7) HAVING GROUPING(t1.col7) < SUM(t1.col4)),
    GROUPING(t1.col6) = ALL (SELECT 1),
    GROUPING(t1.col6) = ALL (SELECT SUM(t1.col7)),
    SUM(t1.col6) = ALL (SELECT GROUPING(t1.col7)),
    GROUPING(t1.col6) = ALL (SELECT GROUPING(t1.col7)),
    GROUPING(t1.col6) = ALL (SELECT GROUPING(t1.col7) FROM tbl_ProductSales)
FROM another_T t1
GROUP BY CUBE(t1.col6, t1.col7);

SELECT
    NOT col1 * col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MAX(col2) > 2),
    NOT AVG(col2) * col1 <> ANY (SELECT 20 FROM tbl_ProductSales HAVING MAX(col1) IS NULL),
    NOT EXISTS (SELECT ColID - 12 FROM tbl_ProductSales GROUP BY ColID HAVING MAX(col1) IS NOT NULL OR MIN(col1) < MIN(col2)),
    CAST (NOT col1 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) | CAST (col2 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER),
    CAST (EXISTS (SELECT MAX(col5) * MAX(col4) FROM another_T GROUP BY col5, col4) AS INTEGER) & CAST (GROUPING(col1, col5) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2) AS INTEGER)
FROM another_T
GROUP BY CUBE(col1, col2, col5);

SELECT
    NOT col1 * col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MAX(col2) > 2) AS a1,
    NOT AVG(col2) * col1 <> ANY (SELECT 20 FROM tbl_ProductSales HAVING MAX(col1) IS NULL) AS a2,
    NOT EXISTS (SELECT ColID - 12 FROM tbl_ProductSales GROUP BY ColID HAVING MAX(col1) IS NOT NULL OR MIN(col1) < MIN(col2)) AS a3,
    CAST (NOT col1 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) | CAST (col2 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) AS a4,
    CAST (EXISTS (SELECT MAX(col5) * MAX(col4) FROM another_T GROUP BY col5, col4) AS INTEGER) & CAST (GROUPING(col1, col5) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2) AS INTEGER) AS a5
FROM another_T
GROUP BY CUBE(col1, col2, col5)
ORDER BY a1 NULLS FIRST, a2 NULLS FIRST, a3 NULLS FIRST, a4 NULLS FIRST, a5 NULLS FIRST;

SELECT
    GROUPING(col1, col2, col3, col4, col5, col6, col7, col8), AVG(col1), CAST(SUM(col2) * 3 AS BIGINT), col3 + col4,
    CAST(MAX(col5) * MIN(col6) AS BIGINT), col7, col1 IN (SELECT ColID FROM tbl_ProductSales), col2 IN (SELECT ColID + col3 FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1, col2, col3, col4, col5, col6, col7, col8) --with 8 columns, a smallint is necessary for grouping's output
ORDER BY GROUPING(col1, col2, col3, col4, col5, col6, col7, col8);

SELECT
    DISTINCT
    NOT col1 IN (SELECT col2 FROM another_T GROUP BY col2)
FROM another_T
GROUP BY ROLLUP(col1);

SELECT
    LAST_VALUE(col5) OVER (PARTITION BY AVG(col8) ORDER BY SUM(col7) NULLS FIRST)
FROM another_T
GROUP BY CUBE(col1, col2, col5, col8);

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
    DISTINCT
    NOT col1 * col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MAX(col2) > 2) AS a1,
    NOT AVG(col2) * col1 <> ANY (SELECT 20 FROM tbl_ProductSales HAVING MAX(col1) IS NULL) AS a2,
    NOT EXISTS (SELECT ColID - 12 FROM tbl_ProductSales GROUP BY ColID HAVING MAX(col1) IS NOT NULL OR MIN(col1) < MIN(col2)) AS a3,
    CAST (NOT col1 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) | CAST (col2 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) AS a4,
    CAST (EXISTS (SELECT MAX(col5) * MAX(col4) FROM another_T GROUP BY col5, col4) AS INTEGER) & CAST (GROUPING(col1, col5) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2) AS INTEGER) AS a5
FROM another_T
GROUP BY CUBE(col1, col2, col5)
ORDER BY a1 NULLS FIRST, a2 NULLS FIRST, a3 NULLS FIRST, a4 NULLS FIRST, a5 NULLS FIRST;

SELECT
    GROUPING(col1, col2, col5, col8) a1,
    col1 IN (SELECT ColID + col2 FROM tbl_ProductSales) a2,
    col1 < ANY (SELECT MAX(ColID + col2) FROM tbl_ProductSales) a3,
    LAST_VALUE(col5) OVER (PARTITION BY AVG(CASE WHEN col8 IS NULL THEN 0 ELSE col8 END) ORDER BY SUM(col7) NULLS FIRST) a4,
    col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MIN(col8) IS NULL) a5,
    EXISTS (SELECT col2 FROM tbl_ProductSales WHERE tbl_ProductSales.ColID = another_T.col1) a6,
    col1 + col5 = (SELECT MIN(ColID) FROM tbl_ProductSales) a7,
    CAST(SUM(DISTINCT CASE WHEN col5 - col8 = (SELECT MIN(ColID / col2) FROM tbl_ProductSales) THEN col2 - 5 ELSE ABS(col1) END) AS BIGINT) a8,
    CAST((SELECT MAX(ColID + col2) FROM tbl_ProductSales) * DENSE_RANK() OVER (PARTITION BY AVG(DISTINCT col5)) AS BIGINT) a9,
    GROUPING(col1, col5, col8) * MIN(col8) OVER (PARTITION BY col5 ORDER BY col1 NULLS LAST ROWS UNBOUNDED PRECEDING) a10,
    MAX(col3) / 10 + GROUPING(col1, col5, col2) * 10 a11,
    GROUP_CONCAT(CAST(col4 AS VARCHAR(32)), '-sep-') || ' plus ' || GROUPING(col1) a12,
    FIRST_VALUE(col1) OVER (ORDER BY col8 DESC NULLS FIRST) a13,
    CAST(col2 * NULL AS BIGINT) a14
FROM another_T
GROUP BY CUBE(col1, col2, col5, col8), GROUPING SETS (())
ORDER BY 
    a1 ASC NULLS FIRST, a2 ASC NULLS LAST, a3 DESC NULLS FIRST, a4 DESC NULLS LAST, a5 ASC NULLS FIRST, 
    a6 DESC NULLS LAST, a7 ASC NULLS FIRST, a8 ASC NULLS LAST, a9 ASC NULLS FIRST, a10 DESC NULLS LAST, 
    a11 ASC NULLS FIRST, a12 DESC NULLS LAST, a13 ASC NULLS FIRST, a14 DESC NULLS LAST;

SELECT
    NOT t1.col1 BETWEEN (SELECT MAX(t1.col7) EXCEPT SELECT tp.ColID FROM tbl_ProductSales tp) AND (SELECT MIN(t1.col5) EXCEPT SELECT t1.col2) a1,
    NOT GROUPING(t1.col1, t1.col2, t1.col4) * RANK() OVER (PARTITION BY AVG(DISTINCT t1.col5)) NOT 
        BETWEEN (SELECT tp2.proj * t1.col1 + MAX(t1.col5) FROM LATERAL (SELECT tp.ColID + MIN(t1.col6) - t1.col1 as proj FROM tbl_ProductSales tp) AS tp2) 
        AND 
        (SELECT SUM(t1.col7) FROM tbl_ProductSales tp HAVING t1.col2 < ALL(SELECT MAX(tp.ColID))) a2
FROM another_T t1
GROUP BY CUBE(t1.col1, t1.col2), GROUPING SETS ((t1.col4))
ORDER BY a1 ASC NULLS FIRST, a2 ASC NULLS LAST; --error, cardinality violation, scalar expression expected

SELECT
    CAST((SELECT tp2.proj * t1.col1 + MAX(t1.col5) FROM LATERAL (SELECT MAX(tp.ColID) + MIN(t1.col6) - t1.col1 as proj FROM tbl_ProductSales tp HAVING NULL IS NOT NULL) AS tp2) AS BIGINT) AS a1
FROM another_T t1
GROUP BY ROLLUP(t1.col1, t1.col2), GROUPING SETS ((t1.col4))
ORDER BY a1 ASC NULLS FIRST;

SELECT
    DISTINCT
    CAST((SELECT tp2.proj * t1.col1 + MAX(t1.col5) FROM LATERAL (SELECT MAX(tp.ColID) + MIN(t1.col6) - t1.col1 as proj FROM tbl_ProductSales tp HAVING NULL IS NOT NULL) AS tp2) AS BIGINT) AS a1
FROM another_T t1
GROUP BY ROLLUP(t1.col1, t1.col2), GROUPING SETS ((t1.col4))
ORDER BY a1 ASC NULLS FIRST;

SELECT 
    NOT GROUPING(t1.col2, t1.col4) <> ALL (SELECT t1.col2 FROM tbl_ProductSales tp WHERE tp.colID = 1) a1
FROM another_T t1
GROUP BY ROLLUP(t1.col1, t1.col2), GROUPING SETS ((t1.col4))
HAVING (t1.col1 = ANY (SELECT MAX(ColID + col2) FROM tbl_ProductSales)) NOT IN 
    ((SELECT NOT EXISTS (SELECT t1.col2 FROM tbl_ProductSales WHERE tbl_ProductSales.ColID = t1.col1)) UNION ALL
     (SELECT NOT GROUPING(t1.col1) BETWEEN (SELECT MAX(t1.col7) EXCEPT SELECT tp.ColID FROM tbl_ProductSales tp) AND (SELECT MIN(t1.col5) EXCEPT SELECT t1.col2)))
ORDER BY a1 DESC NULLS FIRST;

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
    CASE WHEN t1.col1 IN (SELECT 1 FROM tbl_ProductSales tp LEFT JOIN another_T t2 ON tp.ColID = t1.col1) THEN 1 ELSE 2 END,
    CASE WHEN SUM(t1.col3) IN (SELECT MAX(t1.col3) FROM tbl_ProductSales tp LEFT JOIN another_T t2 ON tp.ColID = t1.col1) THEN 1 ELSE 2 END,
    CASE WHEN t1.col2 IN (SELECT MAX(MAX(t1.col3)) OVER (PARTITION BY t1.col2 ORDER BY tp.ColID) FROM tbl_ProductSales tp LEFT JOIN another_T t2 ON tp.ColID = t1.col1) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY ROLLUP(t1.col1, t1.col2);

SELECT
    CASE WHEN t1.col2 IN (SELECT MIN(ColID) FROM tbl_ProductSales tp INNER JOIN another_T t2 ON tp.ColID = t1.col1 AND tp.ColID = t2.col2) THEN 1 ELSE 2 END,
    CASE WHEN t1.col2 IN (SELECT MIN(ColID) FROM tbl_ProductSales tp LEFT JOIN another_T t2 ON tp.ColID = t1.col1 AND tp.ColID = t2.col2) THEN 1 ELSE 2 END,
    CASE WHEN t1.col2 IN (SELECT MIN(ColID) FROM tbl_ProductSales tp RIGHT JOIN another_T t2 ON tp.ColID = t1.col1 AND tp.ColID = t2.col2) THEN 1 ELSE 2 END,
    CASE WHEN t1.col2 IN (SELECT MIN(ColID) FROM tbl_ProductSales tp FULL OUTER JOIN another_T t2 ON tp.ColID = t1.col1 AND tp.ColID = t2.col2) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY ROLLUP(t1.col1, t1.col2);

SELECT
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON t2.col5 = t1.col1)) THEN 1 ELSE 2 END,
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON MIN(t1.col5) = t1.col1)) THEN 1 ELSE 2 END,
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales tp LEFT JOIN another_T t2 ON tp.ColID = t1.col1 AND tp.ColID = t2.col2)) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY CUBE(t1.col1, t1.col2);

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
