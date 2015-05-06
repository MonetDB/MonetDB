START TRANSACTION;

CREATE FUNCTION rapi00() returns table (d integer)
language R {
	return(seq(1,10));
};
SELECT d FROM rapi00() AS R WHERE d > 5;
DROP FUNCTION rapi00;

ROLLBACK;
