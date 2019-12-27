CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

SELECT
    NOT MAX(t1.col6) IN (SELECT SUM(t1.col6) FROM tbl_ProductSales tp HAVING MAX(t1.col1) > MIN(tp.colID))
FROM another_T t1
GROUP BY t1.col6, t1.col7;
	-- True
	-- False
	-- False
	-- False

SELECT
    CAST((SELECT MAX(ColID + col2) FROM tbl_ProductSales) * DENSE_RANK() OVER (PARTITION BY AVG(DISTINCT col5)) AS BIGINT),
    AVG(col1) * MIN(col8) OVER (PARTITION BY col5 ORDER BY col1 ROWS UNBOUNDED PRECEDING) evil,
    CAST(MAX(col3) / 10 + SUM(col1) * 10 AS BIGINT)
FROM another_T
GROUP BY col1, col2, col5, col8;
	-- 6    8       10
	-- 26   968     113
	-- 226  98568   1143
	-- 2226 9874568 11443

SELECT
    CAST (NOT col1 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) | CAST (col2 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER),
    CAST (EXISTS (SELECT MAX(col5) * MAX(col4) FROM another_T GROUP BY col5, col4) AS INTEGER) & CAST (SUM(col4) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2) AS INTEGER)
FROM another_T
GROUP BY col1, col2, col5;
	-- 1	0
	-- 1	0
	-- 1	0
	-- 1	0

SELECT
	EXISTS (SELECT col1 WHERE TRUE),
	EXISTS (SELECT col1 WHERE FALSE),
	EXISTS (SELECT col1 WHERE NULL),
	NOT EXISTS (SELECT col1 WHERE TRUE),
	NOT EXISTS (SELECT col1 WHERE FALSE),
	NOT EXISTS (SELECT col1 WHERE NULL)
FROM another_T t1;
	-- True False False False True True
	-- True False False False True True
	-- True False False False True True
	-- True False False False True True

SELECT
	EXISTS (SELECT AVG(col1) WHERE TRUE),
	EXISTS (SELECT AVG(col1) WHERE FALSE),
	EXISTS (SELECT AVG(col1) WHERE NULL),
	NOT EXISTS (SELECT AVG(col1) WHERE TRUE),
	NOT EXISTS (SELECT AVG(col1) WHERE FALSE),
	NOT EXISTS (SELECT AVG(col1) WHERE NULL)
FROM another_T t1;
	-- The outputs depends if the correlation happens in either inside the inner query or the outer query. However some columns output wrong in MonetDB.
	-- True False False False True True (1x or 4x)

SELECT
	EXISTS (SELECT RANK() OVER (PARTITION BY SUM(DISTINCT col5)))
FROM another_T t1;
	-- True

SELECT
    (SELECT AVG(col1) OVER (PARTITION BY col5 ORDER BY col1 ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales)
FROM another_T t1; --error, more than one row returned by a subquery used as an expression

SELECT
    (SELECT SUM(col2) OVER (PARTITION BY SUM(col2) ORDER BY MAX(col1 + ColID) ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales)
FROM another_T t1
GROUP BY col1; --error, subquery uses ungrouped column "t1.col2" from outer query

SELECT
    (SELECT SUM(SUM(col2)) OVER (PARTITION BY SUM(col2) ORDER BY MAX(col2) ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales)
FROM another_T t1
GROUP BY col1; --error, more than one row returned by a subquery used as an expression

SELECT
    (SELECT DENSE_RANK() OVER (PARTITION BY col5 ORDER BY col1) FROM tbl_ProductSales)
FROM another_T t1; --error, more than one row returned by a subquery used as an expression

SELECT
    (SELECT DENSE_RANK() OVER (PARTITION BY MIN(col5) ORDER BY MAX(col8)) FROM tbl_ProductSales)
FROM another_T t1
GROUP BY col6; --error, more than one row returned by a subquery used as an expression

SELECT
    (SELECT DENSE_RANK() OVER (PARTITION BY MIN(col5) ORDER BY col8 * ColID) FROM tbl_ProductSales)
FROM another_T t1
GROUP BY col6; --error, subquery uses ungrouped column "t1.col8" from outer query

SELECT
    (SELECT t2.col1 * SUM(SUM(t1.col2)) OVER (PARTITION BY SUM(t1.col2) ORDER BY MAX(t1.col1) ROWS UNBOUNDED PRECEDING) FROM another_T t2)
FROM another_T t1
GROUP BY col1; --error, more than one row returned by a subquery used as an expression

SELECT
    (SELECT t2.col1 * SUM(SUM(col2)) OVER (PARTITION BY SUM(col2) ORDER BY MAX(col1) ROWS UNBOUNDED PRECEDING) FROM another_T t2)
FROM another_T t1
GROUP BY col1; --error, column "t2.col1" must appear in the GROUP BY clause or be used in an aggregate function

SELECT
    (SELECT SUM(AVG(ColID)) OVER (PARTITION BY SUM(ColID) ORDER BY MAX(ColID) ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales GROUP BY t1.col1)
FROM another_T t1;
	-- 2,5000
	-- 2,5000
	-- 2,5000
	-- 2,5000

SELECT
    (SELECT SUM(AVG(ColID + col1)) OVER (PARTITION BY SUM(ColID + col3) ORDER BY MAX(ColID) * col4 ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales GROUP BY t1.col1)
FROM another_T t1;
	-- 3,5000
	-- 13,5000
	-- 113,5000
	-- 1113,5000

SELECT
    (SELECT MAX(t1.col2) * SUM(AVG(ColID)) OVER (PARTITION BY SUM(ColID) ORDER BY MAX(ColID) ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales)
FROM another_T t1;
	-- 5555

SELECT
    (SELECT SUM(AVG(ColID)) OVER (PARTITION BY MAX(t1.col2) * SUM(ColID) ORDER BY MAX(ColID) ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales)
FROM another_T t1;
	-- 2,5000

SELECT
    (SELECT SUM(AVG(ColID)) OVER (PARTITION BY SUM(ColID) ORDER BY MAX(t1.col2) * MAX(ColID) ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales)
FROM another_T t1;
	-- 2,5000

SELECT
    (SELECT MAX(ColID) * SUM(AVG(ColID)) OVER (PARTITION BY SUM(ColID) ORDER BY MAX(ColID) ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales)
FROM another_T t1; --MonetDB outputs this one right, but we should leave it here, as it doesn't trigger an error
	-- 10
	-- 10
	-- 10
	-- 10

SELECT
    CAST((SELECT SUM(SUM(col2)) OVER (PARTITION BY SUM(col2) ORDER BY MAX(col1) ROWS UNBOUNDED PRECEDING) FROM another_T) AS BIGINT)
FROM another_T t1
GROUP BY col1; --MonetDB outputs this one right, but we should leave it here, as it doesn't trigger an error
	-- 2468
	-- 2468
	-- 2468
	-- 2468

SELECT
    NOT AVG(col2) * col1 <> ANY (SELECT 20 FROM tbl_ProductSales HAVING MAX(col1) IS NULL) AS a1
FROM another_T
GROUP BY col1, col2, col5
ORDER BY a1 NULLS FIRST;
	-- True
	-- True
	-- True
	-- True

SELECT
    NOT SUM(t1.col2) * MIN(t1.col6 + t1.col6 - t1.col6 * t1.col6) NOT IN (SELECT MAX(t2.col6) FROM another_T t2 GROUP BY t1.col6 HAVING t1.col7 + MIN(t2.col8) < MAX(t2.col7 - t1.col6))
FROM another_T t1
GROUP BY t1.col7, t1.col6;
	-- False
	-- False
	-- False
	-- False

SELECT
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON MIN(t1.col5) = t1.col1)) THEN 1 ELSE 2 END,
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales tp LEFT JOIN another_T t2 ON tp.ColID = t1.col1 AND tp.ColID = t2.col2)) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY t1.col1, t1.col2;
	-- 2	2
	-- 2	2
	-- 2	2
	-- 2	2

SELECT
    SUM(t1.col6) <> ANY (SELECT t1.col7 INTERSECT SELECT t1.col6)
FROM another_T t1
GROUP BY t1.col7, t1.col6;
	-- False
	-- False
	-- False
	-- False

SELECT
    CASE WHEN t1.col1 IN (SELECT 1 FROM tbl_ProductSales tp LEFT JOIN another_T t2 ON tp.ColID = t1.col1) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY t1.col1;
	-- 1
	-- 2
	-- 2
	-- 2

SELECT
    1
FROM another_T t1
GROUP BY t1.col1, t1.col2, t1.col4
HAVING (t1.col1 = ANY (SELECT MAX(ColID + col2) FROM tbl_ProductSales)) NOT IN 
    ((SELECT NOT EXISTS (SELECT t1.col2 FROM tbl_ProductSales WHERE tbl_ProductSales.ColID = t1.col1)) UNION ALL
     (SELECT NOT t1.col1 BETWEEN (SELECT MAX(t1.col7) EXCEPT SELECT tp.ColID FROM tbl_ProductSales tp) AND (SELECT MIN(t1.col5) EXCEPT SELECT t1.col2)));
	-- 1
	-- 1
	-- 1

SELECT
    1
FROM another_T t1
GROUP BY t1.col1, t1.col2, t1.col4
HAVING (t1.col1 = ANY (SELECT MAX(ColID + col2) FROM tbl_ProductSales)) <
    ((SELECT NOT EXISTS (SELECT t1.col2 FROM tbl_ProductSales WHERE tbl_ProductSales.ColID = t1.col1)) INTERSECT
     (SELECT NOT t1.col1 IN (SELECT MAX(t1.col7) EXCEPT SELECT tp.ColID FROM tbl_ProductSales tp)));
	-- 1
	-- 1
	-- 1

SELECT
    col6,
    col7,
    NOT SUM(t1.col6) NOT IN (SELECT MAX(t2.col6) FROM another_T t2 GROUP BY t1.col6 HAVING t1.col7 < MAX(t1.col6))
FROM another_T t1
GROUP BY t1.col7, t1.col6;
	-- 6    7    False
	-- 666  777  False
	-- 6666 7777 False
	-- 66   77   False

SELECT
    col6,
    col7,
    NOT SUM(t1.col6) NOT IN (SELECT MAX(t2.col6) FROM another_T t2 GROUP BY t1.col6 HAVING t1.col7 < MAX(t2.col7 - t1.col6))
FROM another_T t1
GROUP BY t1.col7, t1.col6;
	-- 6    7    False
	-- 666  777  False
	-- 6666 7777 False
	-- 66   77   False

SELECT
    CASE WHEN NULL IN (SELECT MIN(ColID) FROM tbl_ProductSales tp LEFT JOIN another_T t2 ON tp.ColID = t1.col1) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY t1.col1, t1.col2;
	-- 2
	-- 2
	-- 2
	-- 2

SELECT
    CASE WHEN NULL NOT IN (SELECT 1 FROM tbl_ProductSales tp FULL OUTER JOIN another_T t2 ON tp.ColID = t1.col1) THEN 1 ELSE 2 END
FROM another_T t1;
	-- 2
	-- 2
	-- 2
	-- 2

SELECT 
	MIN(i1.i)
FROM integers i1
GROUP BY (SELECT MAX(i2.i) FROM integers i2 LEFT JOIN integers i3 on i1.i = i2.i);
	-- 1

SELECT
	MAX(t1.col1)
FROM another_T t1
GROUP BY (NOT t1.col6 NOT IN (SELECT MAX(t2.col6) FROM another_T t2 GROUP BY t1.col6 HAVING t1.col7 < MAX(t2.col7 - t1.col6)))
HAVING (MIN(t1.col7) <> ANY(SELECT MAX(t2.col5) FROM another_T t2 GROUP BY t2.col6 HAVING t2.col6 + MIN(t2.col2) = MAX(t1.col7)));
	-- empty

SELECT
	1
FROM integers i1
GROUP BY (VALUES(1));
	-- 1

SELECT
	MIN(i1.i)
FROM integers i1
GROUP BY (SELECT SUM(i1.i + i2.i) FROM integers i2);

SELECT
	MIN(i1.i)
FROM integers i1
GROUP BY (SELECT i2.i FROM integers i2); --error, more than one row returned by a subquery used as an expression

SELECT
    (SELECT SUM(t1.col1) OVER (PARTITION BY (VALUES(1)) ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales)
FROM another_T t1; --error, subqueries not allowed inside PARTITION BY

SELECT
    (SELECT SUM(t1.col1) OVER (ORDER BY (VALUES(1)) ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales)
FROM another_T t1;

SELECT
    (SELECT SUM(t1.col1) OVER (ORDER BY (SELECT SUM(t1.col1 + t2.col1) FROM another_T t2) ROWS UNBOUNDED PRECEDING) FROM tbl_ProductSales)
FROM another_T t1;

SELECT
    CAST(SUM(CAST(SUM(CAST (NOT t1.col1 IN (SELECT 1) AS INTEGER)) < ANY (SELECT 1) AS INT)) OVER () AS BIGINT)
FROM another_T t1
GROUP BY t1.col6;
	-- 1
	-- 1
	-- 1
	-- 1

SELECT
    SUM(SUM(t1.col7) * CAST (NOT t1.col1 IN (SELECT 1) AS INTEGER)) OVER ()
FROM another_T t1
GROUP BY t1.col7; --error, column "t1.col1" must appear in the GROUP BY clause or be used in an aggregate function

SELECT
    SUM(CAST(SUM(t1.col6 * CAST (NOT t1.col1 IN (SELECT t2.col2 FROM another_T t2 GROUP BY t2.col2) AS INTEGER)) < ANY (SELECT MAX(ColID + t1.col7 - t1.col2) FROM tbl_ProductSales) AS INT)) OVER (PARTITION BY SUM(t1.col5) ORDER BY (SELECT MIN(t1.col6 + t1.col5 - t2.col2) FROM another_T t2))
FROM another_T t1
GROUP BY t1.col7, t1.col6; --error, subquery uses ungrouped column "t1.col2" from outer query

SELECT
    (SELECT 1 FROM integers i2 GROUP BY SUM(i1.i))
FROM integers i1; --The sum at group by is a correlation from the outer query, so it's allowed inside the GROUP BY at this case
	-- 1

SELECT
    (SELECT 1 FROM integers i2 GROUP BY SUM(i2.i))
FROM integers i1; --error, aggregates not allowed in group by clause

SELECT 
    (SELECT SUM(SUM(i1.i) + i2.i) FROM integers i2 GROUP BY i2.i)
FROM integers i1; --SUM(i1.i) is a correlation from the outer query, so the sum aggregates can be nested at this case
	--error, more than one row returned by a subquery used as an expression

SELECT 
    (SELECT SUM(SUM(i1.i)) FROM integers i2 GROUP BY i2.i)
FROM integers i1; --error, more than one row returned by a subquery used as an expression

SELECT 
    (SELECT SUM(SUM(i2.i)) FROM integers i2 GROUP BY i2.i)
FROM integers i1; --error, aggregation functions cannot be nested

/* We shouldn't allow the following internal functions/procedures to be called from regular queries */
--SELECT "identity"(col1) FROM another_T;
--SELECT "rowid"(col1) FROM another_T;
--SELECT "in"(true, true) FROM another_T;
--SELECT "rotate_xor_hash"(1, 1, 1) FROM another_T;
--CALL sys_update_schemas();
--CALL sys_update_tables();

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
DROP TABLE integers;
