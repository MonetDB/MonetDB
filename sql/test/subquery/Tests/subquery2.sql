
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
