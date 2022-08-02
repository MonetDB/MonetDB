START TRANSACTION;

CREATE TABLE rval(i integer,j integer);
INSERT INTO rval VALUES (1,4), (2,3), (3,2), (4,1);

CREATE FUNCTION rapi02(i integer,j integer,z integer) returns integer
language R {
	return(i*sum(j)*z);
};
SELECT rapi02(i,j,2) as r02 FROM rval;
DROP FUNCTION rapi02;
DROP TABLE rval;

ROLLBACK;
