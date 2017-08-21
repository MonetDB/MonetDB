--YIELD statements inside while loops before statements
CREATE FUNCTION factory03() RETURNS INT BEGIN
    DECLARE a INT;
    SET a = 1;
    WHILE a < 4 DO
        YIELD a;
        SET a = a + 1;
    END WHILE;
END;

SELECT factory03();
SELECT factory03();
SELECT factory03();
SELECT factory03(); --error

DROP FUNCTION factory03;
