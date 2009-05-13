
start transaction;
CREATE TEMPORARY TABLE y (x int);
INSERT INTO y VALUES (13);
SELECT * FROM y;
DROP TABLE y;
rollback;

start transaction;
CREATE TEMPORARY TABLE y (x int);
INSERT INTO y VALUES (13);
SELECT * FROM y;
DROP TABLE y;
rollback;

start transaction;
CREATE TEMPORARY TABLE y (x int);
INSERT INTO y VALUES (13);
SELECT * FROM y;
rollback;

start transaction;
CREATE TEMPORARY TABLE y (x int);
INSERT INTO y VALUES (13);
SELECT * FROM y;
rollback;

start transaction;
CREATE TEMPORARY TABLE y (x int) on commit drop;
INSERT INTO y VALUES (13);
SELECT * FROM y;
rollback;

start transaction;
CREATE TEMPORARY TABLE y (x int) on commit drop;
INSERT INTO y VALUES (13);
SELECT * FROM y;
rollback;
