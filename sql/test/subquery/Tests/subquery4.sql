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

SELECT
	(SELECT i2.i FROM evilfunction(MIN(i1.i)) as i2(i))
FROM integers i1;
	-- 1

SELECT
	(SELECT i2.i FROM evilfunction((SELECT MIN(1))) as i2(i))
FROM integers i1;
	-- 1
	-- 1
	-- 1
	-- 1

SELECT
	(SELECT 1,1 UNION ALL SELECT 2,2)
FROM integers i1; --error, subquery must return only one column

SELECT i FROM integers i1 ORDER BY (SELECT 1 UNION ALL SELECT 2); --error, more than one row returned by a subquery used as an expression

SELECT
	(SELECT 1 UNION ALL SELECT 2)
FROM integers i1; --error, more than one row returned by a subquery used as an expression

SELECT
	(SELECT i2.i FROM evilfunction(MIN(1)) as i2(i))
FROM integers i1; -- error, aggregate functions are not allowed in functions in FROM

SELECT
	(SELECT i2.i FROM evilfunction(MAX(i1.i) OVER ()) as i2(i))
FROM integers i1; -- error, window functions are not allowed in functions in FROM

SELECT
	(SELECT i2.i FROM evilfunction((SELECT MIN(i1.i + i3.i) FROM integers i3)) as i2(i))
FROM integers i1;
	-- 2
	-- 3
	-- 4
	-- NULL

SELECT 1 FROM evilfunction((SELECT MAX(1) OVER ()));
	-- 1

SELECT 1 FROM evilfunction((SELECT MAX(1) OVER () UNION ALL SELECT 1));

SELECT 
	(SELECT 1 FROM evilfunction((SELECT MAX(1) OVER () UNION ALL SELECT 1)))
FROM integers i1; --error, more than one row returned by a subquery used as an expression

SELECT i2.i FROM evilfunction((SELECT MAX(1) OVER ())) as i2(i);
	-- 1

SELECT
	(SELECT i2.i FROM evilfunction((SELECT MAX(i1.i) OVER ())) as i2(i))
FROM integers i1;
	-- 1
	-- 2
	-- 3
	-- NULL

SELECT i FROM integers WHERE (SELECT 1 UNION ALL SELECT 2); --error, more than one row returned by a subquery used as an expression

SELECT i FROM integers WHERE (SELECT true UNION ALL SELECT false); --error, more than one row returned by a subquery used as an expression

SELECT i FROM integers WHERE (SELECT true, false); --error, subquery must return only one column

SELECT i FROM integers WHERE (SELECT true, false UNION ALL SELECT false, true); --error, subquery must return only one column

SELECT i FROM integers WHERE (SELECT COUNT(1) OVER ()) = 1;
	-- 1
	-- 2
	-- 3
	-- NULL

SELECT i FROM integers WHERE (SELECT COUNT(i) OVER ()) = 1;
	-- 1
	-- 2
	-- 3

SELECT
	(SELECT MAX(i2.i) FROM (SELECT MIN(i1.i)) AS i2(i))
FROM integers i1;
	-- 1

SELECT (SELECT NTILE(i1.i) OVER ()) FROM integers i1;
	-- 1
	-- 1
	-- 1
	-- NULL

SELECT (SELECT NTILE(i1.i) OVER (PARTITION BY i1.i)) FROM integers i1;
	-- 1
	-- 1
	-- 1
	-- NULL

UPDATE another_T SET col1 = MIN(col1); --error, aggregates not allowed in update set clause
UPDATE another_T SET col2 = 1 WHERE col1 = SUM(col2); --error, aggregates not allowed in update set clause
UPDATE another_T SET col3 = (SELECT MAX(col5)); --error, aggregates not allowed in update set clause
UPDATE another_T SET col4 = (SELECT SUM(col4 + ColID) FROM tbl_ProductSales); --4 rows affected

SELECT col4 FROM another_T;
	-- 26
	-- 186
	-- 1786
	-- 17786

UPDATE another_T SET col5 = 1 WHERE col5 = (SELECT AVG(col2)); --error, aggregates not allowed in where clause
UPDATE another_T SET col6 = 1 WHERE col6 = (SELECT COUNT(col3 + ColID) FROM tbl_ProductSales);
UPDATE another_T SET col8 = (SELECT 1 FROM integers i2 WHERE AVG(i2.i)); --error, aggregates not allowed in update set clause
UPDATE another_T SET col7 = 1 WHERE col5 = (SELECT 1 FROM integers i2 WHERE AVG(i2.i)); --error, aggregates not allowed in where clause

DELETE FROM another_T WHERE col1 = COUNT(col2); --error, aggregates not allowed in where clause
DELETE FROM another_T WHERE col7 = (SELECT MIN(col3)); --error, aggregates not allowed in where clause
DELETE FROM another_T WHERE col8 = (SELECT AVG(col6 + ColID) FROM tbl_ProductSales); --0 rows affected
DELETE FROM another_T WHERE col2 = (SELECT 1 FROM integers i2 WHERE AVG(i2.i)); --error, aggregates not allowed in where clause

UPDATE another_T SET col1 = AVG(col1) OVER (); --error, window functions not allowed in update set clause
UPDATE another_T SET col2 = 1 WHERE col1 = COUNT(col2) OVER (); --error, window functions not allowed in where clause
UPDATE another_T SET col3 = (SELECT SUM(col5) OVER ()); --4 rows affected

SELECT col3 FROM another_T;
	-- 5
	-- 55
	-- 555
	-- 5555

UPDATE another_T SET col4 = (SELECT MIN(col4 + ColID) OVER () FROM tbl_ProductSales); --error, more than one row returned by a subquery used as an expression
UPDATE another_T SET col5 = 1 WHERE col5 = (SELECT MAX(col2) OVER ()); --0 rows affected
UPDATE another_T SET col6 = 1 WHERE col6 = (SELECT MIN(col3 + ColID) OVER () FROM tbl_ProductSales); --error, more than one row returned by a subquery used as an expression

DELETE FROM another_T WHERE col1 = AVG(col2) OVER (); --error, window functions not allowed in where clause
DELETE FROM another_T WHERE col7 = (SELECT SUM(col3) OVER ()); --0 rows affected
DELETE FROM another_T WHERE col8 = (SELECT MAX(col6 + ColID) OVER () FROM tbl_ProductSales); --error, more than one row returned by a subquery used as an expression

UPDATE another_T SET col5 = (SELECT 1 UNION ALL SELECT 2); --error, more than one row returned by a subquery used as an expression
UPDATE another_T SET col5 = 1 WHERE col5 = (SELECT 1 UNION ALL SELECT 2); --error, more than one row returned by a subquery used as an expression
DELETE FROM another_T WHERE col1 = (SELECT 1 UNION ALL SELECT 2); --error, more than one row returned by a subquery used as an expression
INSERT INTO another_T VALUES ((SELECT 1 UNION ALL SELECT 2),2,3,4,5,6,7,8); --error, more than one row returned by a subquery used as an expression

UPDATE another_T SET (col5, col6) = (SELECT MIN(10), MAX(col5) OVER (PARTITION BY col1)); --4 rows affected

SELECT col5, col6 FROM another_T; --Postgresql uses the updated value of col5 to update col6, but MonetDB and SQLite use the old value of col5, which makes more sense
	-- 10 5
	-- 10 55
	-- 10 555
	-- 10 5555

UPDATE another_T SET (col7, col8) = (SELECT 1,2 UNION ALL SELECT 1,2); --error, more than one row returned by a subquery used as an expression
UPDATE another_T SET (col7, col8) = (SELECT 1 UNION ALL SELECT 2); --error, number of columns does not match number of values
UPDATE another_T SET (col7, col8) = (SELECT 1,2,3); --error, number of columns does not match number of values
UPDATE another_T SET col5 = 1, col5 = 6; --error, multiple assignments to same column "col5"
UPDATE another_T SET (col5, col6) = ((select 1,2)), col5 = 6; --error, multiple assignments to same column "col5"
UPDATE another_T SET (col5, col6) = (SELECT MIN(col1), MAX(col2)); --error, aggregate functions are not allowed in UPDATE

UPDATE another_T SET col7 = (SELECT NTILE(col1) OVER ()); --4 rows affected

SELECT col7 FROM another_T;
	-- 1
	-- 1
	-- 1
	-- 1

UPDATE another_T SET (col5, col6) = (SELECT NTILE(col1) OVER (), MAX(col3) OVER (PARTITION BY col4)); --4 rows affected
UPDATE another_T t1 SET (col1, col2) = (SELECT MIN(t1.col3 + tb.ColID), MAX(tb.ColID) FROM tbl_ProductSales tb); --4 rows affected
UPDATE another_T t1 SET (col3, col4) = (SELECT COUNT(tb.ColID), SUM(tb.ColID) FROM tbl_ProductSales tb); --4 rows affected

SELECT col1, col2, col3, col4, col5, col6 FROM another_T;

DECLARE x int;
SET x = MAX(1) over (); --error, not allowed
DECLARE y int;
SET y = MIN(1); --error, not allowed

INSERT INTO another_T VALUES (SUM(1),2,3,4,5,6,7,8); --error, not allowed
INSERT INTO another_T VALUES (AVG(1) OVER (),2,3,4,5,6,7,8); --error, not allowed
INSERT INTO another_T VALUES ((SELECT SUM(1)),(SELECT SUM(2) OVER ()),3,4,5,6,7,8); --allowed

SELECT * FROM another_T;

CREATE PROCEDURE crashme(a int) BEGIN DECLARE x INT; SET x = a; END;

CALL crashme(COUNT(1)); --error, not allowed
CALL crashme(COUNT(1) OVER ()); --error, not allowed

CALL crashme((SELECT COUNT(1))); --error, subquery at CALL
CALL crashme((SELECT COUNT(1) OVER ())); --error, subquery at CALL
CALL crashme((SELECT 1 UNION ALL SELECT 2)); --error, subquery at CALL

SELECT row_number(1) OVER () FROM integers i1; --error, row_number(int) doesn't exist
SELECT ntile(1,1) OVER () FROM integers i1; --error, ntile(int,int) doesn't exist

create sequence "debugme" as integer start with 1;
alter sequence "debugme" restart with (select MAX(1));
alter sequence "debugme" restart with (select MIN(1) OVER ());
drop sequence "debugme";

CREATE FUNCTION upsme(input INT) RETURNS INT BEGIN RETURN SELECT MIN(input) OVER (); END;

SELECT upsme(1);
SELECT upsme(1);

DROP FUNCTION upsme(INT);
DROP FUNCTION evilfunction(INT);
DROP FUNCTION evilfunction(INT, INT);
DROP PROCEDURE crashme(INT);
DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
DROP TABLE integers;
