CREATE TABLE a (name VARCHAR(10));
CREATE TABLE b (name VARCHAR(10));
INSERT INTO a VALUES ('a'),('b');
INSERT INTO b VALUES ('a'),('b');
CREATE TABLE ab AS SELECT * FROM a,b WITH DATA;
drop table b;
drop table a;
