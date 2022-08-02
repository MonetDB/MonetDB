CREATE TABLE test3832("State" string, "Sales" double);
INSERT INTO test3832 VALUES('Texus', 200);
INSERT INTO test3832 VALUES('Texas', 250);

SELECT LEFT("State", 3) AS "State",  SUM("Sales") AS "Sales" FROM test3832 GROUP BY "State";
-- 2 rows
SELECT LEFT("State", 3) AS "State",  SUM("Sales") AS "Sales" FROM test3832 GROUP BY LEFT("State", 3);
-- syntax error, unexpected LEFT in: "select left("State", 3) as "State",  sum("Sales") as "Sales" from test3832 group"
SELECT LEFT("State", 3) AS "State3", SUM("Sales") AS "Sales" FROM test3832 GROUP BY "State3";
-- 1 row

DROP TABLE test3832;


CREATE TABLE fields (name varchar(30) NOT NULL, tablenm varchar(30) NOT NULL, pos int NOT NULL);
INSERT into fields VALUES ('c1 ', 't1 ', 1), ('c2 ', 't1 ', 2), ('c4 ', 't1 ', 4), ('c3 ', 't1 ', 3), ('c2 ', 't2', 2), ('c4 ', 't2', 4), ('c1 ', 't2', 1), ('c3 ', 't3', 3);
INSERT into fields SELECT RTRIM(name) AS name, RTRIM(tablenm) AS tablenm, pos FROM fields;
SELECT * FROM fields ORDER BY tablenm, pos, name;

SELECT name, COUNT(*) as count, MAX(pos) as max_pos, MIN(pos) as min_pos FROM fields GROUP BY name ORDER BY name;
SELECT RTRIM(name) as nametrimmed, COUNT(*) as count, MAX(pos) as max_pos, MIN(pos) as min_pos FROM fields GROUP BY name ORDER BY RTRIM(name);
SELECT RTRIM(name) as nametrimmed, COUNT(*) as count, MAX(pos) as max_pos, MIN(pos) as min_pos FROM fields GROUP BY RTRIM(name) ORDER BY RTRIM(name);
-- syntax error, unexpected '(', expecting SCOLON in: "select rtrim(name), count(*) as count, max(pos) as max_pos, min(pos) as min_pos "
SELECT RTRIM(name) as nametrimmed, COUNT(*) as count, MAX(pos) as max_pos, MIN(pos) as min_pos FROM fields GROUP BY nametrimmed ORDER BY nametrimmed;
SELECT RTRIM(name) as nametrimmed, COUNT(*) as count, MAX(pos) as max_pos, MIN(pos) as min_pos FROM (SELECT RTRIM(name) AS name, pos FROM fields) AS fromquery GROUP BY name ORDER BY RTRIM(name);

DROP TABLE fields;

