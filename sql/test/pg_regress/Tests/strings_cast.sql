--
-- STRINGS
-- Test various data entry syntaxes.
--

--
-- test conversions between various string types
-- E021-10 implicit casting among the character data types
--
CREATE TABLE CHAR_TBL(f1 char(4));
INSERT INTO CHAR_TBL (f1) VALUES ('a');
INSERT INTO CHAR_TBL (f1) VALUES ('ab');
INSERT INTO CHAR_TBL (f1) VALUES ('abcd');

CREATE TABLE VARCHAR_TBL(f1 varchar(4));
INSERT INTO VARCHAR_TBL (f1) VALUES ('a');
INSERT INTO VARCHAR_TBL (f1) VALUES ('ab');
INSERT INTO VARCHAR_TBL (f1) VALUES ('abcd');

CREATE TABLE TEXT_TBL (f1 text);
INSERT INTO TEXT_TBL VALUES ('doh!');
INSERT INTO TEXT_TBL VALUES ('hi de ho neighbor');


SELECT CAST(f1 AS text) AS "text(char)" FROM CHAR_TBL;

SELECT CAST(f1 AS text) AS "text(varchar)" FROM VARCHAR_TBL;

SELECT CAST('namefield' AS text) AS "text(name)";

-- since this is an explicit cast, it should truncate w/o error:
SELECT CAST(f1 AS char(10)) AS "char(text)" FROM TEXT_TBL;
-- note: implicit-cast case is tested in char.sql

SELECT CAST(f1 AS char(20)) AS "char(text)" FROM TEXT_TBL;

SELECT CAST(f1 AS char(10)) AS "char(varchar)" FROM VARCHAR_TBL;

SELECT CAST('namefield' AS char(10)) AS "char(name)";

SELECT CAST(f1 AS varchar(3)) AS "varchar(text)" FROM TEXT_TBL;

SELECT CAST(f1 AS varchar(3)) AS "varchar(char)" FROM CHAR_TBL;

SELECT CAST('namefield' AS varchar(3)) AS "varchar(name)";


-- cleanup
DROP TABLE CHAR_TBL;
DROP TABLE VARCHAR_TBL;
DROP TABLE TEXT_TBL;
