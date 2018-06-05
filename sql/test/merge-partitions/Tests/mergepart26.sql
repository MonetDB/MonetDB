CREATE MERGE TABLE testnestedtables (a int, b varchar(32)) PARTITION BY RANGE USING (a + 1);
CREATE MERGE TABLE subnested1 (a int, b varchar(32)) PARTITION BY VALUES USING (a - 1);
CREATE TABLE sub1 (a int, b varchar(32));
CREATE TABLE sub2 (a int, b varchar(32));

ALTER TABLE subnested1 ADD TABLE sub2 AS PARTITION IN (9, 11, 13, 15);
ALTER TABLE testnestedtables ADD TABLE subnested1 AS PARTITION BETWEEN -100 AND 0;

INSERT INTO sub1 VALUES (2, 'going'), (4, 'very'), (6, 'good');
ALTER TABLE subnested1 ADD TABLE sub1 AS PARTITION IN (1, 3, 5, 7); --error

ALTER TABLE testnestedtables DROP TABLE subnested1;
ALTER TABLE subnested1 DROP TABLE sub2;

DROP TABLE sub1;
DROP TABLE sub2;
DROP TABLE subnested1;
DROP TABLE testnestedtables;

CREATE MERGE TABLE testagain (a int, b int) PARTITION BY VALUES USING (b * 2 + 3);
CREATE MERGE TABLE subnn (a int, b int) PARTITION BY RANGE USING (a + 2);
CREATE TABLE sub1 (a int, b int);
CREATE TABLE sub2 (a int, b int);

ALTER TABLE subnn ADD TABLE sub2 AS PARTITION BETWEEN 50 AND 300;
ALTER TABLE testagain ADD TABLE subnn AS PARTITION IN (7, 9, 10, 11, 12, 13);

INSERT INTO sub1 VALUES (2, 2), (4, 4), (5, 5), (6, 6);
ALTER TABLE subnn ADD TABLE sub1 AS PARTITION BETWEEN 0 AND 8; --error

ALTER TABLE testagain DROP TABLE subnn;

ALTER TABLE testagain ADD TABLE subnn AS PARTITION IN (7, 11, 13, 15);
ALTER TABLE subnn ADD TABLE sub1 AS PARTITION BETWEEN 0 AND 8;

ALTER TABLE testagain DROP TABLE subnn;
ALTER TABLE subnn DROP TABLE sub1;
ALTER TABLE subnn DROP TABLE sub2;

DROP TABLE sub1;
DROP TABLE sub2;
DROP TABLE subnn;
DROP TABLE testagain;
