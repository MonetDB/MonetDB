CREATE MERGE TABLE table1 (b varchar(32)) PARTITION BY RANGE ON (b);
CREATE MERGE TABLE table2 (b varchar(32)) PARTITION BY VALUES ON (b);
CREATE TABLE another (b varchar(32));

ALTER TABLE table1 ADD TABLE another; --error, a paritioning clause is required
ALTER TABLE table2 ADD TABLE another; --error, a paritioning clause is required

DROP TABLE another;
DROP TABLE table1;
DROP TABLE table2;

CREATE MERGE TABLE table1 (a int) PARTITION BY RANGE ON (a);
CREATE TABLE another1 (a int);
CREATE TABLE another2 (a int);

ALTER TABLE table1 ADD TABLE another1 AS PARTITION FROM RANGE MINVALUE TO RANGE MAXVALUE WITH NULL VALUES; --holds all
INSERT INTO table1 VALUES (1), (NULL);
INSERT INTO another1 VALUES (2), (NULL);

SELECT a FROM table1;
SELECT a FROM another1;

ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM RANGE MINVALUE TO RANGE MAXVALUE; --error, conflicts with another1
ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM RANGE MINVALUE TO RANGE MAXVALUE WITH NULL VALUES; --error, conflicts with another1
ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM 1 TO 2; --error, conflicts with another1
ALTER TABLE table1 ADD TABLE another2 AS PARTITION FOR NULL VALUES; --error, conflicts with another1

ALTER TABLE table1 SET TABLE another1 AS PARTITION FROM 1 TO 2; --error, there are NULL values
ALTER TABLE table1 SET TABLE another1 AS PARTITION FROM 1 TO 3 WITH NULL VALUES;

ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM 3 TO 4 WITH NULL VALUES; --error, conflicts with another1
ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM 3 TO 4;

ALTER TABLE table1 DROP TABLE another1;
ALTER TABLE table1 DROP TABLE another2;

DELETE FROM another1 WHERE a IS NULL;
ALTER TABLE table1 ADD TABLE another1 AS PARTITION FROM RANGE MINVALUE TO RANGE MAXVALUE;
ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM 1 TO 2; --error, conflicts with another1
ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM RANGE MINVALUE TO -1; --error, conflicts with another1
ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM 10 TO RANGE MAXVALUE; --error, conflicts with another1
ALTER TABLE table1 ADD TABLE another2 AS PARTITION FOR NULL VALUES;

TRUNCATE table1;
INSERT INTO table1 VALUES (2), (NULL);  

INSERT INTO another1 VALUES (3);
INSERT INTO another1 VALUES (NULL); --error
INSERT INTO another2 VALUES (2); --error
INSERT INTO another2 VALUES (NULL);

SELECT a FROM table1;
SELECT a FROM another1;
SELECT a FROM another2;

ALTER TABLE table1 DROP TABLE another1;
ALTER TABLE table1 DROP TABLE another2;
DROP TABLE another1;
DROP TABLE another2;
DROP TABLE table1;
