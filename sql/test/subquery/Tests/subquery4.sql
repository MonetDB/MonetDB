CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);
CREATE FUNCTION evilfunction(input INT) RETURNS TABLE (outt INT) BEGIN RETURN TABLE(SELECT input); END;
CREATE FUNCTION evilfunction(input1 INT, input2 INT) RETURNS TABLE (outt INT) BEGIN RETURN TABLE(SELECT input1 + input2); END;

PREPARE SELECT
	(SELECT ? FROM evilfunction((SELECT 1))) 
FROM another_T;

PREPARE SELECT
	(SELECT 1 FROM evilfunction((SELECT ?))) 
FROM another_T;

PREPARE SELECT
	(SELECT 1 FROM evilfunction((SELECT ?, ?))) 
FROM another_T;

UPDATE another_T SET col1 = MIN(col1); --error, aggregates not allowed in update set clause
UPDATE another_T SET col2 = 1 WHERE col1 = SUM(col2); --error, aggregates not allowed in update set clause
UPDATE another_T SET col3 = (SELECT MAX(col5)); --error, aggregates not allowed in update set clause
UPDATE another_T SET col4 = (SELECT SUM(col4 + ColID) FROM tbl_ProductSales);
UPDATE another_T SET col5 = 1 WHERE col5 = (SELECT AVG(col2)); --error, aggregates not allowed in where clause
UPDATE another_T SET col6 = 1 WHERE col6 = (SELECT COUNT(col3 + ColID) FROM tbl_ProductSales);

DELETE FROM another_T WHERE col1 = COUNT(col2); --error, aggregates not allowed in where clause
DELETE FROM another_T WHERE col7 = (SELECT MIN(col3)); --error, aggregates not allowed in where clause
DELETE FROM another_T WHERE col8 = (SELECT AVG(col6 + ColID) FROM tbl_ProductSales);

UPDATE another_T SET col1 = AVG(col1) OVER (); --error, window functions not allowed in update set clause
UPDATE another_T SET col2 = 1 WHERE col1 = COUNT(col2) OVER (); --error, window functions not allowed in where clause
UPDATE another_T SET col3 = (SELECT SUM(col5) OVER ());
UPDATE another_T SET col4 = (SELECT MIN(col4 + ColID) OVER () FROM tbl_ProductSales); --error, more than one row returned by a subquery used as an expression
UPDATE another_T SET col5 = 1 WHERE col5 = (SELECT MAX(col2) OVER ());
UPDATE another_T SET col6 = 1 WHERE col6 = (SELECT MIN(col3 + ColID) OVER () FROM tbl_ProductSales); --error, more than one row returned by a subquery used as an expression

DELETE FROM another_T WHERE col1 = AVG(col2) OVER (); --error, window functions not allowed in where clause
DELETE FROM another_T WHERE col7 = (SELECT SUM(col3) OVER ());
DELETE FROM another_T WHERE col8 = (SELECT MAX(col6 + ColID) OVER () FROM tbl_ProductSales); --error, more than one row returned by a subquery used as an expression

DROP FUNCTION evilfunction(INT);
DROP FUNCTION evilfunction(INT, INT);
DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
DROP TABLE integers;
