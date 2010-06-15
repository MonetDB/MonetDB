CREATE TABLE a1 (name VARCHAR(10));
CREATE TABLE b1 (name VARCHAR(10));
INSERT INTO a1 VALUES ('a'),('b');
INSERT INTO b1 VALUES ('a'),('b');
SELECT a1.name as x, b1.name as y FROM a1,b1 WHERE a1.name LIKE b1.name;
DROP table b1;
DROP table a1;
