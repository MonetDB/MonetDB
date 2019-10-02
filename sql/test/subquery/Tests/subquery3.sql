CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

SELECT
    NOT MAX(t1.col6) IN (SELECT SUM(t1.col6) FROM tbl_ProductSales tp HAVING MAX(t1.col1) > MIN(tp.colID))
FROM another_T t1
GROUP BY t1.col6, t1.col7;
	-- True
	-- False
	-- False
	-- False

SELECT
    (SELECT MAX(ColID + col2) FROM tbl_ProductSales) * DENSE_RANK() OVER (PARTITION BY AVG(DISTINCT col5)),
    AVG(col1) * MIN(col8) OVER (PARTITION BY col5 ORDER BY col1 ROWS UNBOUNDED PRECEDING) evil,
    MAX(col3) / 10 + SUM(col1) * 10
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

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
