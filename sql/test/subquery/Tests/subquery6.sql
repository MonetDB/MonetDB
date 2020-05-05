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

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
DROP TABLE integers;
