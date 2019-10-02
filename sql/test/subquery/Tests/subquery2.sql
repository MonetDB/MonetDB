
CREATE TABLE students(id INTEGER, name VARCHAR(128), major VARCHAR(128), "year" INTEGER);
CREATE TABLE exams(sid INTEGER, course VARCHAR(128), curriculum VARCHAR(128), grade INTEGER, "year" INTEGER);
INSERT INTO students VALUES (1, 'Mark', 'CS', 2017);
INSERT INTO students VALUES (2, 'Dirk', 'CS', 2017);
INSERT INTO exams VALUES (1, 'Database Systems', 'CS', 10, 2015);
INSERT INTO exams VALUES (1, 'Graphics', 'CS', 9, 2016);
INSERT INTO exams VALUES (2, 'Database Systems', 'CS', 7, 2015);
INSERT INTO exams VALUES (2, 'Graphics', 'CS', 7, 2016);

SELECT s.name, e.course, e.grade FROM students s, exams e WHERE s.id=e.sid AND e.grade=(SELECT MAX(e2.grade) FROM exams e2 WHERE s.id=e2.sid) ORDER BY name, course;
	-- Dirk, Database Systems, 7
	-- Dirk, Graphics, 7
	-- Mark, Database Systems, 10

SELECT s.name, e.course, e.grade FROM students s, exams e WHERE s.id=e.sid AND (s.major = 'CS' OR s.major = 'Games Eng') AND e.grade <= (SELECT AVG(e2.grade) - 1 FROM exams e2 WHERE s.id=e2.sid OR (e2.curriculum=s.major AND s."year">=e2."year")) ORDER BY name, course;
	-- Dirk, Database Systems, 7
	-- Dirk, Graphics, 7

SELECT name, major FROM students s WHERE EXISTS(SELECT * FROM exams e WHERE e.sid=s.id AND grade=10) OR s.name='Dirk' ORDER BY name;
	-- Dirk, CS
	-- Mark, CS

drop table students;
drop table exams;

CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

SELECT col1 IN (SELECT ColID + col1 FROM tbl_ProductSales) FROM another_T GROUP BY col1; 
	-- False
	-- False
	-- False
	-- False

SELECT col1 IN (SELECT SUM(ColID + col1) FROM tbl_ProductSales) FROM another_T GROUP BY col1;
	-- False
	-- False
	-- False
	-- False

SELECT (SELECT col2 FROM tbl_ProductSales WHERE tbl_ProductSales.ColID = another_T.col1) FROM another_T GROUP BY col1, col2;
	-- NULL
	-- NULL
	-- 2
	-- NULL

SELECT
	EXISTS (SELECT col2 FROM tbl_ProductSales WHERE tbl_ProductSales.ColID = another_T.col1)
FROM another_T GROUP BY col1, col2, col5, col8;
	-- True
	-- False
	-- False
	-- False

SELECT
	EXISTS (SELECT col2 FROM tbl_ProductSales WHERE tbl_ProductSales.ColID = another_T.col1),
	(SELECT ColID FROM tbl_ProductSales) * DENSE_RANK() OVER (PARTITION BY AVG(DISTINCT col5))
FROM another_T GROUP BY col1, col2, col5, col8; --error, more than one row returned by a subquery used as an expression 

SELECT
	-col1 IN (SELECT ColID FROM tbl_ProductSales),
	col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MIN(col8) IS NULL)
FROM another_T GROUP BY col1, col2, col5, col8;
	-- False True
	-- False True
	-- False True
	-- False True

SELECT
	col1 + col5 = (SELECT MIN(ColID) FROM tbl_ProductSales),
	CAST(SUM(DISTINCT CASE WHEN col5 - col8 = (SELECT MIN(ColID / col2) FROM tbl_ProductSales) THEN col2 - 5 ELSE ABS(col1) END) AS BIGINT),
	(SELECT MAX(ColID + col2) FROM tbl_ProductSales) * DENSE_RANK() OVER (PARTITION BY AVG(DISTINCT col5))
FROM another_T
GROUP BY col1, col2, col5, col8;
	-- False 1    6
	-- False 11   26
	-- False 111  226
	-- False 1111 2226

SELECT
	col1 + col5 = (SELECT MIN(ColID) FROM tbl_ProductSales),
	MIN(col8) OVER (PARTITION BY col5 ORDER BY col1 ROWS UNBOUNDED PRECEDING)
FROM another_T
GROUP BY col1, col2, col5, col8;
	-- False 8
	-- False 88
	-- False 888
	-- False 8888

SELECT
	EXISTS (SELECT 1 FROM tbl_ProductSales),
	NOT EXISTS (SELECT 1 FROM tbl_ProductSales)
FROM another_T
GROUP BY col1;
	-- True False
	-- True False
	-- True False
	-- True False

-- TODO incorrect empty result
SELECT NOT col2 <> ANY (SELECT 20 FROM tbl_ProductSales GROUP BY ColID HAVING NOT MAX(col1) <> col1 * AVG(col1 + ColID) * ColID) FROM another_T GROUP BY col1, col2, col5, col8;

SELECT
	NOT -SUM(col2) NOT IN (SELECT ColID FROM tbl_ProductSales GROUP BY ColID HAVING SUM(ColID - col8) <> col5),
	NOT col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MAX(col8) > 2 AND MIN(col8) IS NOT NULL),
--	NOT col2 <> ANY (SELECT 20 FROM tbl_ProductSales GROUP BY ColID HAVING NOT MAX(col1) <> col1 * AVG(col1 + ColID) * ColID),
	NOT EXISTS (SELECT ColID - 12 FROM tbl_ProductSales GROUP BY ColID HAVING MAX(col2) IS NULL OR NOT col8 <> 2 / col1)
FROM another_T
GROUP BY col1, col2, col5, col8;
	-- False True True True
	-- False True True True
	-- False True True True
	-- False True True True

SELECT
	DISTINCT
	NOT col1 * col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MAX(col2) > 2),
	NOT AVG(col2) * col1 <> ANY (SELECT 20 FROM tbl_ProductSales HAVING MAX(col1) IS NOT NULL OR MIN(col1) < MIN(col2)),
	CAST (NOT col1 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) | CAST (col2 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER),
	CAST (EXISTS (SELECT MAX(col5) * MAX(col4) FROM another_T GROUP BY col5, col4) AS INTEGER) & CAST (AVG(col1) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2) AS INTEGER)
FROM another_T
GROUP BY col1, col2, col5;
	-- False False 1 0
	-- True  False 1 0

SELECT
	SUM(col1) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2)
FROM another_T
GROUP BY col4;
	-- False
	-- False
	-- False
	-- False

SELECT
	(SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t1.col5 = t2.col1)
FROM another_T t1;
	-- NULL
	-- NULL
	-- NULL
	-- NULL

SELECT
	t1.col1 = ALL (SELECT col4 + SUM(t1.col5) FROM another_T INNER JOIN tbl_ProductSales ON another_T.col1 = tbl_ProductSales.ColID)
FROM another_T t1
GROUP BY t1.col1;
	-- False
	-- False
	-- False
	-- False

SELECT
	SUM(t1.col6) NOT IN (SELECT t1.col7),
	t1.col6 NOT IN (SELECT t1.col7),
	t1.col6 IN (SELECT SUM(t1.col7)),
	t1.col6 IN (SELECT SUM(t1.col7) FROM tbl_ProductSales)
FROM another_T t1
GROUP BY t1.col6, t1.col7;
	-- True True False False
	-- True True False False
	-- True True False False
	-- True True False False

SELECT
	(SELECT MAX(col6) FROM tbl_ProductSales) IN (SELECT MIN(col3) FROM another_T)
FROM another_T
GROUP BY col1; --error, subquery returns more than 1 row

SELECT
	SUM(col3 + col2)
FROM another_T
GROUP BY col1
HAVING NOT col1 = ANY (SELECT 0 FROM tbl_ProductSales GROUP BY ColID HAVING NOT MAX(col1) <> AVG(col1));
	-- 5
	-- 555
	-- 55
	-- 5555

-- TODO incorrect empty result
SELECT
	SUM(col3) * col1
FROM another_T
GROUP BY col1
HAVING NOT col1 <> ANY (SELECT 0 FROM tbl_ProductSales GROUP BY ColID HAVING NOT MAX(col1) <> col1 * AVG(col1 + ColID) * ColID);
	-- 3
	-- 36963
	-- 363
	-- 3702963

SELECT
	SUM(CAST(t1.col1 IN (SELECT t1.col1 FROM another_T) AS INTEGER))
FROM another_T t1
GROUP BY t1.col2;
	-- 1
	-- 1
	-- 1
	-- 1

-- TODO incorrect empty result
SELECT
    (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t1.col7 <> SOME(SELECT MAX(t1.col1 + t3.col4) FROM another_T t3))
FROM another_T t1;
	-- 1
	-- 1
	-- 1
	-- 1

-- 4x NULL vs postgress wrong with 1x NULL
SELECT
	CASE WHEN 1 IN (SELECT (SELECT MAX(col7)) UNION ALL (SELECT MIN(ColID) FROM tbl_ProductSales INNER JOIN another_T t2 ON t2.col5 = t2.col1)) THEN 2 ELSE NULL END
FROM another_T t1;	
	-- NULL

SELECT
	CASE WHEN NOT col1 NOT IN (SELECT (SELECT MAX(col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON t2.col5 = t1.col1)) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY col1;
	-- 1
	-- 2
	-- 2
	-- 2

SELECT
	t1.col1 IN (SELECT ColID FROM tbl_ProductSales GROUP BY t1.col1, tbl_ProductSales.ColID)
FROM another_T t1
GROUP BY col1;
	-- True
	-- False
	-- False
	-- False

SELECT t1.col1 FROM another_T t1 WHERE t1.col1 >= ANY(SELECT t1.col1 + t2.col1 FROM another_T t2); --empty result

INSERT INTO tbl_ProductSales VALUES (0, 'a', 'b', 0);
SELECT col1 IN (SELECT ColID + col1 FROM tbl_ProductSales) FROM another_T GROUP BY col1; 
	-- True
	-- True
	-- True
	-- True

SELECT col1 IN (SELECT col1 * SUM(ColID + col1) FROM tbl_ProductSales) FROM another_T GROUP BY col1;
	-- False
	-- False
	-- False
	-- False

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
