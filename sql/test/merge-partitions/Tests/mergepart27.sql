CREATE MERGE TABLE checkreadonly (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subt1 (a int, b varchar(32));

ALTER TABLE subt1 SET READ ONLY;
ALTER TABLE checkreadonly ADD TABLE subt1 AS PARTITION FROM 1 TO 100;
INSERT INTO checkreadonly VALUES (1, 'wrong'); --error
ALTER TABLE subt1 SET READ WRITE;
INSERT INTO checkreadonly VALUES (1, 'ok');

ALTER TABLE checkreadonly DROP TABLE subt1;
DROP TABLE checkreadonly;
DROP TABLE subt1;

CREATE MERGE TABLE checksequence (a int auto_increment, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subt1 (a int auto_increment, b varchar(32));
ALTER TABLE checksequence ADD TABLE subt1 AS PARTITION FROM 1 TO 100; --error not compatible sequences
DROP TABLE checksequence;
DROP TABLE subt1;

CREATE MERGE TABLE checkdefault (a int default '1', b clob default 'something') PARTITION BY RANGE ON (a);
CREATE TABLE subt1 (a int default '2', b clob default 'else');
CREATE TABLE subt2 (a int default '1', b clob default 'something');

ALTER TABLE checkdefault ADD TABLE subt1 AS PARTITION FROM 1 TO 100; --error not compatible defaults
ALTER TABLE checkdefault ADD TABLE subt2 AS PARTITION FROM 1 TO 100;

INSERT INTO checkdefault VALUES (DEFAULT, DEFAULT);

ALTER TABLE checkdefault DROP TABLE subt2;
DROP TABLE checkdefault;
DROP TABLE subt1;
DROP TABLE subt2;

CREATE MERGE TABLE checkkeys (a int PRIMARY KEY, b varchar(32)) PARTITION BY RANGE USING (b || 'ups'); --error, primary not on a partitioned column
CREATE MERGE TABLE checkkeys (a int, b varchar(32) PRIMARY KEY) PARTITION BY RANGE USING (a + 1); --error, primary not on a partitioned column

CREATE MERGE TABLE checkkeys (a int, b int, PRIMARY KEY(a, b)) PARTITION BY RANGE USING (a + 1); --error, primary not on a partitioned column
CREATE MERGE TABLE checkkeys (a int, b int, PRIMARY KEY(a, b)) PARTITION BY RANGE USING (a + b + 1);
DROP TABLE checkkeys;

CREATE MERGE TABLE checkkeys (a int PRIMARY KEY, b varchar(32)) PARTITION BY RANGE ON (b); --error, primary not on a partitioned column
CREATE MERGE TABLE checkkeys (a int, b varchar(32) PRIMARY KEY) PARTITION BY RANGE ON (a); --error, primary not on a partitioned column
CREATE MERGE TABLE checkkeys (a int PRIMARY KEY, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE referenceme (mememe int PRIMARY KEY);
CREATE TABLE otherref (othermeme varchar(32) PRIMARY KEY);

ALTER TABLE checkkeys ADD FOREIGN KEY (a) REFERENCES referenceme (mememe);
ALTER TABLE checkkeys ADD FOREIGN KEY (b) REFERENCES otherref (othermeme); --foreign keys on non-partitioned columns is allowed
ALTER TABLE checkkeys DROP CONSTRAINT checkkeys_a_fkey;

CREATE TABLE subt1 (a int PRIMARY KEY, b varchar(32));
CREATE TABLE subt2 (a int, b varchar(32) PRIMARY KEY);

ALTER TABLE checkkeys ADD TABLE subt1 AS PARTITION FROM 1 TO 100; --error, doesn't have the same foreign key
ALTER TABLE subt1 ADD FOREIGN KEY (b) REFERENCES otherref (othermeme);
ALTER TABLE checkkeys ADD TABLE subt1 AS PARTITION FROM 1 TO 100;
ALTER TABLE subt1 DROP CONSTRAINT subt1_b_fkey; --error, cannot drop SQL constraints while the table is part of a merge table
ALTER TABLE subt1 ADD FOREIGN KEY (a) REFERENCES referenceme (mememe); --error, cannot add SQL constraints while the table is part of a merge table
ALTER TABLE checkkeys ADD TABLE subt2 AS PARTITION FROM 101 TO 200; --error, primary keys don't match

ALTER TABLE checkkeys DROP CONSTRAINT checkkeys_b_fkey; --error, merge table has child tables
ALTER TABLE checkkeys ADD FOREIGN KEY (a) REFERENCES referenceme (mememe); --error, merge table has child tables
ALTER TABLE checkkeys ADD FOREIGN KEY (b) REFERENCES otherref (othermeme); --error, merge table has child tables

ALTER TABLE checkkeys DROP TABLE subt1;
ALTER TABLE subt1 DROP CONSTRAINT subt1_b_fkey;

ALTER TABLE checkkeys DROP CONSTRAINT checkkeys_b_fkey;
ALTER TABLE checkkeys ADD FOREIGN KEY (b) REFERENCES otherref (othermeme);

CREATE TABLE subt3 (a int PRIMARY KEY, b varchar(32), FOREIGN KEY (a) REFERENCES referenceme(mememe));
CREATE TABLE another (mememe int PRIMARY KEY);

ALTER TABLE checkkeys ADD TABLE subt3 AS PARTITION FROM 1 TO 100; --error checkkeys does not have the foreign key b
ALTER TABLE checkkeys ADD FOREIGN KEY (a) REFERENCES referenceme (mememe);
ALTER TABLE checkkeys DROP CONSTRAINT checkkeys_b_fkey;
ALTER TABLE checkkeys ADD TABLE subt3 AS PARTITION FROM 1 TO 100;
ALTER TABLE checkkeys DROP TABLE subt3;
ALTER TABLE checkkeys DROP CONSTRAINT checkkeys_a_fkey;
ALTER TABLE subt3 DROP CONSTRAINT subt3_a_fkey;

ALTER TABLE subt3 ADD FOREIGN KEY (a) REFERENCES another (mememe);
ALTER TABLE checkkeys ADD TABLE subt3 AS PARTITION FROM 1 TO 100; --error foreign keys reference different tables

CREATE MERGE TABLE checkunique (a int unique, b varchar(32)) PARTITION BY RANGE ON (b); --error, partition by on a not unique column
CREATE MERGE TABLE checkunique (a int unique, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subt4 (a int, b varchar(32) unique);
ALTER TABLE checkunique ADD TABLE subt4 AS PARTITION FROM 1 TO 2; --error, the partition is not on the unique column
DROP TABLE subt4;

CREATE TABLE subt4 (a int unique , b varchar(32));
ALTER TABLE checkunique ADD TABLE subt4 AS PARTITION FROM 1 TO 2;
ALTER TABLE checkunique DROP TABLE subt4;

DROP TABLE checkkeys;
DROP TABLE checkunique;
DROP TABLE subt1;
DROP TABLE subt2;
DROP TABLE subt3;
DROP TABLE subt4;
DROP TABLE referenceme;
DROP TABLE otherref;
DROP TABLE another;
