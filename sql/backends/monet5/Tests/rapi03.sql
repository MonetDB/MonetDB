START TRANSACTION;

CREATE TABLE rval(i integer);
INSERT INTO rval VALUES (1),(2),(3),(4),(-1),(0);

CREATE FUNCTION rapi03(i integer,z integer) returns boolean language R {i>z};
SELECT * FROM rval WHERE rapi03(i,2);
DROP FUNCTION rapi03;
DROP TABLE rval;


ROLLBACK;
