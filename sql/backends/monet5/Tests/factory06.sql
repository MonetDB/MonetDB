--Update the factory function parameter in the body
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

-- Update the factory function parameter in the body
-- This variant works
CREATE FUNCTION factory06a(param INT) RETURNS INT BEGIN
    declare p int;
    set p = param;
    YIELD p;
    SET p = p + 1;
    YIELD p;
    SET p = p + 1;
    YIELD p;
END;

SELECT factory06a(1);
SELECT factory06a(1);
SELECT factory06a(1);
SELECT factory06a(1); --error

DROP FUNCTION factory06a;
