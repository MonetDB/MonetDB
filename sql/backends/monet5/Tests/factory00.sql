# Test MAL factories with YIELD statements

CREATE FUNCTION factory1() RETURNS INT BEGIN
    YIELD 1;
    YIELD 2;
    YIELD 3;
END;

SELECT factory1();
SELECT factory1();
SELECT factory1();
SELECT factory1(); --error

CREATE FUNCTION factory2() RETURNS INT BEGIN
    DECLARE a INT;
    SET a = 1;
    YIELD a;
    SET a = a + 1;
    YIELD a;
    SET a = a + 1;
    YIELD a;
END;

SELECT factory2();
SELECT factory2();
SELECT factory2();
SELECT factory2(); --error

CREATE FUNCTION factory3() RETURNS INT BEGIN
    DECLARE a INT;
    SET a = 0;
    WHILE a < 4 DO
        SET a = a + 1;
        YIELD a;
    END WHILE;
END;

SELECT factory3();
SELECT factory3();
SELECT factory3();
SELECT factory3(); --error

CREATE FUNCTION factory4() RETURNS INT BEGIN
    DECLARE a INT;
    SET a = 1;
    WHILE a < 4 DO
        YIELD a;
        SET a = a + 1;
    END WHILE;
END;

SELECT factory4();
SELECT factory4();
SELECT factory4();
SELECT factory4(); --error

CREATE FUNCTION factory5() RETURNS INT BEGIN
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

SELECT factory5();
SELECT factory5();
SELECT factory5();
SELECT factory5(); --error

DROP FUNCTION factory1;
DROP FUNCTION factory2;
DROP FUNCTION factory3;
DROP FUNCTION factory4;
DROP FUNCTION factory5;
