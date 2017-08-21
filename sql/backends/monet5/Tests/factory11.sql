CREATE FUNCTION factory11() RETURNS VARCHAR(32) BEGIN
    DECLARE aa VARCHAR(32);
    SET aa = 'This is a string! :) :) :) :) :)';
    YIELD aa;
    SET aa = SUBSTRING(aa, 0, 16);
    YIELD aa;
    SET aa = aa || aa;
    YIELD aa;
    SET aa = NULL;
    YIELD aa;
END;

SELECT factory11();
SELECT factory11();
SELECT factory11();
SELECT factory11();
SELECT factory11(); --error
SELECT factory11();
SELECT factory11();
SELECT factory11();
SELECT factory11();
SELECT factory11(); --error

DROP FUNCTION factory11;
