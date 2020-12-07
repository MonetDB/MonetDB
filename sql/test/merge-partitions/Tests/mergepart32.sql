CREATE MERGE TABLE table1 (a int) PARTITION BY RANGE ON (a);
CREATE TABLE another1 (a int);
CREATE TABLE another2 (a int);
CREATE TABLE another3 (a int);

ALTER TABLE table1 ADD TABLE another1 AS PARTITION FROM 10 TO 10;

insert into table1 values (10);
insert into table1 values (11); --error
insert into another1 values (11); --error
insert into another1 values (10);

ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM 10 TO 11; --error, conflicts with another1
ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM 11 TO 11;
ALTER TABLE table1 ADD TABLE another3 AS PARTITION FROM 11 TO 12; --error, conflicts with another2
ALTER TABLE table1 ADD TABLE another3 AS PARTITION FROM 10 TO RANGE MAXVALUE; --error, conflicts with another1
ALTER TABLE table1 ADD TABLE another3 AS PARTITION FROM 11 TO RANGE MAXVALUE; --error, conflicts with another2

insert into table1 values (11);
insert into table1 values (10);
insert into another2 values (10); --error
insert into another2 values (11);

SELECT * FROM table1;
SELECT * FROM another1;
SELECT * FROM another2;

ALTER TABLE table1 DROP TABLE another1;
ALTER TABLE table1 DROP TABLE another2;

DROP TABLE another1;
DROP TABLE another2;
DROP TABLE another3;
DROP TABLE table1;
