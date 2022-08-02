START TRANSACTION;

CREATE FUNCTION rapi01(i integer) returns table (i integer, d double)
language R {
	return(data.frame(i=seq(1,i),d=42.0));
};
SELECT i,d FROM rapi01(42) AS R WHERE i>40;
DROP FUNCTION rapi01;

ROLLBACK;
