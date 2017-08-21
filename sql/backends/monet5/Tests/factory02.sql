--YIELD statements inside while loops after statements
CREATE FUNCTION factory02() RETURNS INT BEGIN
    DECLARE a INT;
    SET a = 0;
    WHILE a < 3 DO
        SET a = a + 1;
        YIELD a;
    END WHILE;
END;

SELECT factory02();
SELECT factory02();
SELECT factory02();
SELECT factory02(); --error

DROP FUNCTION factory02;
