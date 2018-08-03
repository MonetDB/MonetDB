CREATE MERGE TABLE checkreadonly (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subt1 (a int, b varchar(32));

ALTER TABLE subt1 SET READ ONLY;
ALTER TABLE checkreadonly ADD TABLE subt1 AS PARTITION BETWEEN 1 AND 100;
INSERT INTO checkreadonly VALUES (1, 'wrong'); --error
ALTER TABLE subt1 SET READ WRITE;
INSERT INTO checkreadonly VALUES (1, 'ok');

ALTER TABLE checkreadonly DROP TABLE subt1;
DROP TABLE checkreadonly;
DROP TABLE subt1;

CREATE MERGE TABLE checksequence (a int auto_increment, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subt1 (a int auto_increment, b varchar(32));
ALTER TABLE checksequence ADD TABLE subt1 AS PARTITION BETWEEN 1 AND 100; --error not compatible sequences
DROP TABLE checksequence;
DROP TABLE subt1;

CREATE MERGE TABLE checkdefault (a int default '1', b clob default 'something') PARTITION BY RANGE ON (a);
CREATE TABLE subt1 (a int default '2', b clob default 'else');
CREATE TABLE subt2 (a int default '1', b clob default 'something');

ALTER TABLE checkdefault ADD TABLE subt1 AS PARTITION BETWEEN 1 AND 100; --error not compatible defaults
ALTER TABLE checkdefault ADD TABLE subt2 AS PARTITION BETWEEN 1 AND 100;

INSERT INTO checkdefault VALUES (DEFAULT, DEFAULT);

ALTER TABLE checkdefault DROP TABLE subt2;
DROP TABLE checkdefault;
DROP TABLE subt1;
DROP TABLE subt2;

CREATE MERGE TABLE checkkeys (a int PRIMARY KEY, b varchar(32)) PARTITION BY RANGE USING (b || 'ups'); --error
CREATE MERGE TABLE checkkeys (a int, b varchar(32) PRIMARY KEY) PARTITION BY RANGE USING (a + 1); --error

CREATE MERGE TABLE checkkeys (a int, b int, PRIMARY KEY(a, b)) PARTITION BY RANGE USING (a + 1); --error
CREATE MERGE TABLE checkkeys (a int, b int, PRIMARY KEY(a, b)) PARTITION BY RANGE USING (a + b + 1);
DROP TABLE checkkeys;

CREATE MERGE TABLE checkkeys (a int PRIMARY KEY, b varchar(32)) PARTITION BY RANGE ON (b); --error
CREATE MERGE TABLE checkkeys (a int, b varchar(32) PRIMARY KEY) PARTITION BY RANGE ON (a); --error
CREATE MERGE TABLE checkkeys (a int PRIMARY KEY, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE referenceme (mememe int PRIMARY KEY);
CREATE TABLE otherref (othermeme varchar(32) PRIMARY KEY);

ALTER TABLE checkkeys ADD FOREIGN KEY (a) REFERENCES referenceme (mememe);
ALTER TABLE checkkeys ADD FOREIGN KEY (b) REFERENCES otherref (othermeme); --error not compatible key with partition
ALTER TABLE checkkeys DROP CONSTRAINT checkkeys_a_fkey;

CREATE TABLE subt1 (a int PRIMARY KEY, b varchar(32));
CREATE TABLE subt2 (a int, b varchar(32) PRIMARY KEY);

ALTER TABLE checkkeys ADD TABLE subt1 AS PARTITION BETWEEN 1 AND 100;
ALTER TABLE checkkeys ADD TABLE subt2 AS PARTITION BETWEEN 101 AND 200; --error

ALTER TABLE checkkeys ADD FOREIGN KEY (a) REFERENCES referenceme (mememe); --error
ALTER TABLE checkkeys ADD FOREIGN KEY (b) REFERENCES otherref (othermeme); --error

ALTER TABLE checkkeys DROP TABLE subt1;

CREATE TABLE subt3 (a int PRIMARY KEY, b varchar(32), FOREIGN KEY (a) REFERENCES referenceme(mememe));
CREATE TABLE another (mememe int PRIMARY KEY);

ALTER TABLE checkkeys ADD TABLE subt3 AS PARTITION BETWEEN 1 AND 100; --error checkkeys does not have the foreign key
ALTER TABLE checkkeys ADD FOREIGN KEY (a) REFERENCES referenceme (mememe);
ALTER TABLE checkkeys ADD TABLE subt3 AS PARTITION BETWEEN 1 AND 100;
ALTER TABLE checkkeys DROP TABLE subt3;
ALTER TABLE checkkeys DROP CONSTRAINT checkkeys_a_fkey;
ALTER TABLE subt3 DROP CONSTRAINT subt3_a_fkey;

ALTER TABLE subt3 ADD FOREIGN KEY (a) REFERENCES another (mememe);
ALTER TABLE checkkeys ADD TABLE subt3 AS PARTITION BETWEEN 1 AND 100; --error foreign keys reference different tables

DROP TABLE checkkeys;
DROP TABLE subt1;
DROP TABLE subt2;
DROP TABLE subt3;
DROP TABLE referenceme;
DROP TABLE otherref;
DROP TABLE another;
