CREATE FUNCTION factory06(param INT) RETURNS INT BEGIN
    YIELD param;
    SET param = param + 1;
    YIELD param;
    SET param = param + 1;
    YIELD param;
END;

SELECT factory06(1);
SELECT factory06(1);
SELECT factory06(1);
SELECT factory06(1); --error

DROP FUNCTION factory06;
