--test a factory returning a single column
--leaving a regular function here for comparison
CREATE FUNCTION regfunction15(aa INT) RETURNS TABLE (aa INT) BEGIN
    RETURN TABLE (SELECT aa);
END;

SELECT aa FROM regfunction15(NULL);
SELECT aa FROM regfunction15(1);
SELECT aa FROM regfunction15(2);
SELECT aa FROM regfunction15(3);

-- yield a single column
CREATE FUNCTION factory15(aa INT) RETURNS TABLE (aa INT) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT NULL);
        YIELD TABLE (SELECT aa);
        YIELD TABLE (SELECT aa + 1);
        YIELD TABLE (SELECT aa + 2);
    END WHILE;
END;

SELECT aa FROM factory15(1);
SELECT aa FROM factory15(1);
SELECT aa FROM factory15(1);
SELECT aa FROM factory15(1);

DROP FUNCTION regfunction15;
DROP FUNCTION factory15;
