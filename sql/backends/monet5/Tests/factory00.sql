# Test MAL factories with YIELD statements

CREATE FUNCTION factory00() RETURNS INT BEGIN
    YIELD 1;
    YIELD 2;
    YIELD 3;
END;

SELECT factory00();
SELECT factory00();
SELECT factory00();
SELECT factory00(); --error

DROP FUNCTION factory00;
