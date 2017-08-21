--Recursive factories
CREATE FUNCTION factory21(aa INT) RETURNS INT BEGIN
    WHILE TRUE DO
        YIELD aa;
        YIELD factory21(aa + 1);
    END WHILE;
END;

SELECT factory21(1);
SELECT factory21(1);
SELECT factory21(1);
SELECT factory21(1);
SELECT factory21(1);
SELECT factory21(1);
SELECT factory21(1);

DROP FUNCTION factory21;
