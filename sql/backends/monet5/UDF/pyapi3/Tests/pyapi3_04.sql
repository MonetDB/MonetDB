# Return a couple of boolean values with no parameter provided (i.e. return a constant)
START TRANSACTION;

CREATE TABLE dval(i integer);
INSERT INTO dval VALUES (1),(2),(3),(4);

CREATE FUNCTION pyapi04() returns boolean
language P
{
    return [True] * 4
};

SELECT pyapi04() FROM dval;
DROP FUNCTION pyapi04;
DROP TABLE dval;

ROLLBACK;

