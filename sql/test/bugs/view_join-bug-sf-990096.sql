-- Both these queries should give the same result:

-- View-less version:
START TRANSACTION;

CREATE TABLE x (i integer);
INSERT INTO x VALUES (1);
CREATE TABLE y (i integer);
INSERT INTO y VALUES (1);

SELECT * FROM
	x a INNER JOIN y ON (a.i = y.i)
;

ROLLBACK;

START TRANSACTION;
-- Version with view:
CREATE TABLE x (i integer);
INSERT INTO x VALUES (1);
CREATE VIEW xview AS SELECT * FROM x;

CREATE TABLE y (i integer);
INSERT INTO y VALUES (1);

SELECT a.i FROM
	xview a INNER JOIN y ON (a.i = y.i)
;

ROLLBACK;
