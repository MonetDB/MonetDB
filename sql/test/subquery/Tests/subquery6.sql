CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

DELETE FROM another_t WHERE (SELECT 1 UNION SELECT 2) > 1; 
	--error, more than one row returned by a subquery used as an expression

DELETE FROM another_t WHERE (SELECT 1 UNION SELECT 2) > 1; 
	--error, more than one row returned by a subquery used as an expression

UPDATE another_T SET col1 = 1 WHERE (SELECT 1 UNION SELECT 2) > 1;
	--error, more than one row returned by a subquery used as an expression

MERGE INTO another_t USING (SELECT col1 FROM another_t) sub ON (SELECT 1 UNION SELECT 2) > 1 WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT;
	--error, more than one row returned by a subquery used as an expression

MERGE INTO another_t USING (SELECT (SELECT 1 UNION SELECT 2) FROM another_t) sub ON TRUE WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT;
	--error, more than one row returned by a subquery used as an expression

WITH customer_total_return AS
  (SELECT 1 AS ctr_customer_sk,
          1 AS ctr_state,
          1 AS ctr_total_return)
SELECT 1
FROM customer_total_return ctr1,
     another_T,
     tbl_ProductSales
WHERE ctr1.ctr_total_return >
    (SELECT avg(ctr_total_return)*1.2
     FROM customer_total_return ctr2
     WHERE ctr1.ctr_state = ctr2.ctr_state)
  AND col1 = ColID
  AND ctr1.ctr_customer_sk = TotalSales;
	--empty

SELECT i FROM integers i1 WHERE (SELECT CASE WHEN i1.i IS NULL THEN (SELECT FALSE FROM integers i2) ELSE TRUE END);
	--error, more than one row returned by a subquery used as an expression

SELECT (SELECT (SELECT SUM(col1)) IN (MAX(col2))) FROM another_t;
	-- False

SELECT 1 IN (col4, MIN(col2)) FROM another_t;
	--error, column "another_t.col4" must appear in the GROUP BY clause or be used in an aggregate function

SELECT (SELECT col1) IN ('not a number') FROM another_t;
	-- error, cannot cast string into number

SELECT (SELECT (SELECT SUM(col1)) IN (MAX(col2), '12')) FROM another_t;
	-- False

SELECT CASE WHEN ColID IS NULL THEN CAST(Product_Category AS INT) ELSE TotalSales END FROM tbl_ProductSales;
	-- 200
	-- 400
	-- 500
	-- 100

SELECT ColID FROM tbl_ProductSales WHERE CASE WHEN ColID IS NULL THEN CAST(Product_Category AS INT) ELSE TotalSales END;
	-- 1
	-- 2
	-- 3
	-- 4

SELECT COALESCE(ColID, CAST(Product_Category AS INT)) FROM tbl_ProductSales;
	-- 1
	-- 2
	-- 3
	-- 4

SELECT CAST(SUM((SELECT col1)) AS BIGINT) FROM another_t;
	-- 1234

SELECT CAST(SUM((SELECT col1 + col2)) AS BIGINT) FROM another_t;
	-- 3702

SELECT CAST(SUM((SELECT CAST(EXISTS(SELECT col1) AS INT))) AS BIGINT) FROM another_t;
	-- 4

SELECT CAST(SUM((SELECT (SELECT col1 + col2))) AS BIGINT) FROM another_t;
	-- 3702

SELECT CAST((SELECT SUM((SELECT col1))) AS BIGINT) FROM another_t;
	-- 1234

SELECT CAST((SELECT SUM((SELECT col1 + col2))) AS BIGINT) FROM another_t;
	-- 3702

SELECT (SELECT 1 FROM another_t t1 WHERE 'aa' LIKE t2.product_category) FROM tbl_ProductSales t2;
	-- NULL
	-- NULL
	-- NULL
	-- NULL

SELECT t1.colid FROM tbl_productsales t1 INNER JOIN tbl_productsales t2 ON t1.product_category NOT LIKE t2.product_category ORDER BY t1.colid;

SELECT t1.colid FROM tbl_productsales t1 INNER JOIN tbl_productsales t2 ON t1.product_category NOT LIKE t2.product_name ORDER BY t1.colid;

SELECT (SELECT 1 FROM another_t t1 WHERE t2.product_category LIKE CAST(t1.col1 AS VARCHAR(32))) FROM tbl_ProductSales t2;
	-- NULL
	-- NULL
	-- NULL
	-- NULL

SELECT (SELECT t2.col2 FROM another_t t2 WHERE t1.col1 BETWEEN t2.col1 AND t2.col2) FROM another_t t1;
	-- 2
	-- 22
	-- 222
	-- 2222

SELECT (SELECT t2.col2 FROM another_t t2 WHERE t2.col1 BETWEEN t1.col1 AND t2.col2) FROM another_t t1;
	-- error, more than one row returned by a subquery used as an expression

SELECT (SELECT t2.col2 FROM another_t t2 WHERE t2.col1 BETWEEN t2.col1 AND t1.col2) FROM another_t t1;
	-- error, more than one row returned by a subquery used as an expression

SELECT 1 > (SELECT 2 FROM integers);
	-- error, more than one row returned by a subquery used as an expression

SELECT (SELECT 1) > ANY(SELECT 1);
	-- False

CREATE FUNCTION debugme() RETURNS INT
BEGIN
	DECLARE res INT;
	SET res = 1 > (select 9 from integers);
	RETURN res;
END;
SELECT debugme(); --error, more than one row returned by a subquery used as an expression
DROP FUNCTION debugme;

SELECT i = ALL(i), i < ANY(i), i = ANY(NULL) FROM integers;
	-- True False NULL
	-- True False NULL
	-- True False NULL
	-- NULL NULL  NULL

SELECT i FROM integers WHERE i = ANY(NULL);
	--empty

CREATE FUNCTION debugme2() RETURNS INT
BEGIN
	DECLARE n INT;
	WHILE (1 > (select 9 from integers)) do
		SET n = n -1;
	END WHILE;
	RETURN n;
END;
SELECT debugme2(); --error, more than one row returned by a subquery used as an expression
DROP FUNCTION debugme2;

CREATE FUNCTION debugme3() RETURNS INT
BEGIN
	DECLARE n INT;
	WHILE (1 > ALL(select 1)) do
		SET n = n -1;
	END WHILE;
	RETURN n;
END;
SELECT debugme3();
	--NULL
DROP FUNCTION debugme3;

CREATE FUNCTION debugme4() RETURNS BOOLEAN
BEGIN
	DECLARE n BOOLEAN;
	SET n = (select true union all select false);
	RETURN n;
END;
SELECT debugme4(); --error, more than one row returned by a subquery used as an expression
DROP FUNCTION debugme4;

CREATE FUNCTION debugme5() RETURNS BOOLEAN
BEGIN
	DECLARE n BOOLEAN;
	SET n = (select 1 where null);
	RETURN n;
END;
SELECT debugme5(); --error, cannot fetch a single row from an empty input
DROP FUNCTION debugme5;

CREATE FUNCTION debugme6() RETURNS INT
BEGIN
	DECLARE n INT;
	WHILE ((SELECT 0) = ANY(SELECT 1)) do
		SET n = 10;
	END WHILE;
	RETURN n;
END;
SELECT debugme6();
	--NULL
DROP FUNCTION debugme6;

select rank() over (), min(TotalSales) from tbl_ProductSales;
	-- 1 100

select count(*) over (), max(Product_Name) from tbl_ProductSales;
	-- 1 Shorts

select corr(1,1), corr(1,1) over () from tbl_ProductSales;
	-- NULL NULL

select min(colid), group_concat(null) over () from tbl_ProductSales where null;
	-- NULL NULL

select min(colid), group_concat(null, null) over () from tbl_ProductSales where null;
	-- NULL NULL

select max(colid), group_concat('a', null) over () from tbl_ProductSales where null;
	-- NULL a

select ntile(1) over ();
	-- 1

select ntile(11) over (), min(TotalSales) from tbl_ProductSales;
	-- 1 100

select nth_value('bug', '1') over ();
	-- bug

select nth_value('bug', 1) over ();
	-- bug

select nth_value('bug', 1) over (), max(TotalSales) from tbl_ProductSales;
	-- bug 500

select lag('bug') over (), lag('bug', 1) over (), lag('bug', 0) over (), lag('bug', 1, 'oth') over (), lag('bug', 0, 'oth') over ();
	-- NULL NULL bug oth bug

select lead('bug') over (), lead('bug', 1) over (), lead('bug', 0) over (), lead('bug', 1, 'oth') over (), lead('bug', 0, 'oth') over (), max(TotalSales) from tbl_ProductSales;
	-- NULL NULL bug oth bug 500

select 1 = any(ColID), max(totalsales) from tbl_ProductSales;
	-- error, cannot use non GROUP BY column 'ColID' in query results without an aggregate function

select 1 in (ColID), max(totalsales) from tbl_ProductSales;
	-- error, cannot use non GROUP BY column 'ColID' in query results without an aggregate function

select 1 in (ColID, (select 1)), max(totalsales) from tbl_ProductSales;
	-- error, cannot use non GROUP BY column 'ColID' in query results without an aggregate function

select (select (i1.i, i2.i) in (select i2.i, i1.i) from integers i2) from integers i1;
	-- I expect this to throw the error: IN with multiple columns not available in projections

select min(i) as myx from integers group by myx;
	-- I expect this to throw the error: aggregate functions are not allowed in GROUP BY

select ntile(i) over (), count(*) from integers;
	--error, column "i" must appear in the GROUP BY clause or be used in an aggregate function

select (select i2.i in (select sum(i1.i + i2.i)) from integers i2) from integers i1;
	--error, column "i2.i" must appear in the GROUP BY clause or be used in an aggregate function

select (select i2.i in (select sum(i1.i + i2.i)) from integers i2) from integers i1 group by i1.i;
	--error, column "i2.i" must appear in the GROUP BY clause or be used in an aggregate function

select (select i2.i in (select sum(i1.i + i2.i)) from integers i2 group by i2.i) from integers i1;
	--error, more than one row returned by a subquery used as an expression

select (select i2.i in (select sum(i1.i + i2.i)) from integers i2 group by i2.i) from integers i1 group by i1.i;
	--error, more than one row returned by a subquery used as an expression

select (select sum(i2.i) in (select sum(i1.i + i2.i)) from integers i2 group by i2.i) from integers i1 group by i1.i;
	--error, more than one row returned by a subquery used as an expression

select (select sum(i1.i + i2.i) in (select sum(i1.i + i2.i)) from integers i2 group by i2.i) from integers i1 group by i1.i;
	--error, more than one row returned by a subquery used as an expression

select (select sum(i1.i + i2.i) in (select sum(i1.i + i2.i)) from integers i2) from integers i1;
	-- True
	-- True
	-- True
	-- NULL

select (select sum(i1.i + i2.i) > (select sum(i1.i + i2.i)) from integers i2) from integers i1;
	-- False
	-- False
	-- False
	-- NULL

select (select sum(i1.i) > (select sum(i1.i + i2.i)) from integers i2) from integers i1;
	-- error, subquery uses ungrouped column "i1.i" from outer query

select (select sum(i2.i) > (select sum(i1.i + i2.i)) from integers i2) from integers i1;
	-- False
	-- False
	-- False
	-- NULL

select (select sum(i1.i) > (select sum(i1.i + i2.i)) from integers i2) from integers i1 group by i1.i;
	-- False
	-- False
	-- False
	-- NULL

select (select sum(i1.i) < (select sum(sum(i1.i) + i2.i)) from integers i2) from integers i1;
	-- True

select (select exists (select sum(i1.i + i2.i)) from integers i2) from integers i1;
	-- True
	-- True
	-- True
	-- True

select (select not exists (select sum(i1.i + i2.i)) from integers i2) from integers i1 group by i1.i;
	-- False
	-- False
	-- False
	-- False

select 1 where (select 1 union all select 2) in (1);
	--error, more than one row returned by a subquery used as an expression

select (select 1 union all select 2) in (1);
	--error, more than one row returned by a subquery used as an expression

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
DROP TABLE integers;
