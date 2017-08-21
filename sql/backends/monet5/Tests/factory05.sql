
CREATE FUNCTION factory05(param INT) RETURNS INT BEGIN
    YIELD param;
    YIELD param;
END;

SELECT factory05(1);
SELECT factory05(1);
SELECT factory05(1); --error

DROP FUNCTION factory05;
