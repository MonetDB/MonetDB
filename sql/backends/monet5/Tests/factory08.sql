--YIELD statement inside a IF statement
CREATE FUNCTION factory08() RETURNS INT BEGIN  --show only the even numbers
    DECLARE a INT;
    SET a = 0;
    WHILE TRUE DO
        IF a % 2 = 0 THEN
            YIELD a;
        END IF;
        SET a = a + 1;
    END WHILE;
END;

SELECT factory08();
SELECT factory08();
SELECT factory08();
SELECT factory08();
SELECT factory08();
SELECT factory08();
SELECT factory08();

DROP FUNCTION factory08;
