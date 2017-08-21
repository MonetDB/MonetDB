CREATE FUNCTION factory07() RETURNS INT BEGIN
    DECLARE a INT;
    SET a = 0;
    WHILE TRUE DO
        SET a = a + 1;
        YIELD a;
    END WHILE;
END;

--shall never output an error
SELECT factory07();
SELECT factory07();
SELECT factory07();
SELECT factory07();
SELECT factory07();
SELECT factory07();
SELECT factory07();
SELECT factory07();
SELECT factory07();
SELECT factory07();
SELECT factory07();

DROP FUNCTION factory07;
