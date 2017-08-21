# Test MAL factories with YIELD statements

CREATE FUNCTION factory04() RETURNS INT BEGIN
    DECLARE a INT;
    SET a = 0;
    WHILE a < 1 DO
        SET a = a + 1;
    END WHILE;
    WHILE a < 4 DO
        YIELD a;
        SET a = a + 1;
    END WHILE;
END;

SELECT factory04();
SELECT factory04();
SELECT factory04();
SELECT factory04(); --error

DROP FUNCTION factory04;
