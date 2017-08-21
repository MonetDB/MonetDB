--Check yield statements are allowed in multiple while loops, even nested
CREATE FUNCTION factory09() RETURNS INT BEGIN
    DECLARE a INT;
    DECLARE b INT;
    SET a = 0;
    WHILE a < 10 DO
        IF a % 2 = 0 THEN
            YIELD a;
        END IF;
        SET a = a + 1;
    END WHILE;
    WHILE a < 20 DO
        SET b = a - 4;
        WHILE b < a DO
            IF b % 2 = 0 THEN
                YIELD a;
            ELSE
                YIELD b;
            END IF;
            SET b = b + 1;
        END WHILE;
        SET a = a + 4;
    END WHILE;
END;

SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();
SELECT factory09();

DROP FUNCTION factory09;
