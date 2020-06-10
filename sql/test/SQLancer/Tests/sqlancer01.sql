CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

START TRANSACTION;
-- Bug 6883
select 1 from integers where (((0.7161494983624688) in (integers.i)) = true) = false;
	-- 1
	-- 1
	-- 1

delete from integers where (((0.7161494983624688) in (integers.i)) = true) = false;
	-- 3 rows affected
select i from integers;
	-- NULL
ROLLBACK;

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
DROP TABLE integers;
