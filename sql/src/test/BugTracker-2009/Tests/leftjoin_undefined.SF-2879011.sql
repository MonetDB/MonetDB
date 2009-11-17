CREATE TABLE a (name VARCHAR(10));
CREATE TABLE b (name VARCHAR(10));
INSERT INTO a VALUES ('a'),('b');
INSERT INTO b VALUES ('a'),('b');
SELECT a.name as x, b.name as y FROM a,b WHERE a.name LIKE b.name;
DROP table b;
DROP table a;
