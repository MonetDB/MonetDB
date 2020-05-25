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

SELECT (SELECT col1) IN ('not a number') FROM another_t;
	-- error, cannot cast string into number

SELECT (SELECT (SELECT SUM(col1)) IN (MAX(col2), '12')) FROM another_t;
	-- False

SELECT 1 IN (col4, MIN(col2)) FROM another_t;
	--error, column "another_t.col4" must appear in the GROUP BY clause or be used in an aggregate function

SELECT CASE WHEN ColID IS NULL THEN CAST(Product_Category AS INT) ELSE TotalSales END FROM tbl_ProductSales;
	-- 200
	-- 400
	-- 500
	-- 100

SELECT ColID FROM tbl_ProductSales WHERE CASE WHEN ColID IS NULL THEN CAST(Product_Category AS INT) < 0 ELSE TotalSales > 0 END;
	-- 1
	-- 2
	-- 3
	-- 4

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
DROP TABLE integers;
