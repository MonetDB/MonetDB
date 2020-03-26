CREATE MERGE TABLE table1 (b varchar(32)) PARTITION BY RANGE ON (b);
CREATE MERGE TABLE table2 (b varchar(32)) PARTITION BY VALUES ON (b);
CREATE TABLE another (b varchar(32));

ALTER TABLE table1 ADD TABLE another; --error, a paritioning clause is required
ALTER TABLE table2 ADD TABLE another; --error, a paritioning clause is required

DROP TABLE another;
DROP TABLE table1;
DROP TABLE table2;
