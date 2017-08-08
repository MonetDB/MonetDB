
CREATE FUNCTION factory6(param INT) RETURNS INT BEGIN
    YIELD param;
    YIELD param;
END;

SELECT factory6(1);
SELECT factory6(1);
SELECT factory6(1); --error

CREATE FUNCTION factory7(param INT) RETURNS INT BEGIN
    YIELD param;
    SET param = param + 1;
    YIELD param;
    SET param = param + 1;
    YIELD param;
END;

SELECT factory7(0);
SELECT factory7(0);
SELECT factory7(0);
SELECT factory7(0); --error

CREATE FUNCTION factory8() RETURNS INT BEGIN
    DECLARE a INT;
    SET a = 0;
    WHILE TRUE DO
        SET a = a + 1;
        YIELD a;
    END WHILE;
END;

--shall never output an error
SELECT factory8();
SELECT factory8();
SELECT factory8();
SELECT factory8();
SELECT factory8();
SELECT factory8();
SELECT factory8();
SELECT factory8();
SELECT factory8();
SELECT factory8();
SELECT factory8();

CREATE FUNCTION factory9() RETURNS INT BEGIN  --show only the even numbers
    DECLARE a INT;
    SET a = 0;
    WHILE TRUE DO
        IF a % 2 = 0 THEN
            YIELD a;
        END IF;
        SET a = a + 1;
    END WHILE;
END;

SELECT factory9();
SELECT factory9();
SELECT factory9();
SELECT factory9();
SELECT factory9();
SELECT factory9();
SELECT factory9();

CREATE FUNCTION factory10() RETURNS INT BEGIN
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
        SET a = a + 1;
    END WHILE;
END;

SELECT factory10();
SELECT factory10();
SELECT factory10();
SELECT factory10();
SELECT factory10();
SELECT factory10();
SELECT factory10();

DROP FUNCTION factory7;
DROP FUNCTION factory8;
DROP FUNCTION factory9;
DROP FUNCTION factory10;
