--leaving a regular function here
CREATE FUNCTION regfunction(aa INT, bb CLOB) RETURNS TABLE (aa INT, bb CLOB) BEGIN
    RETURN TABLE (SELECT aa, bb);
END;

SELECT aa, bb FROM regfunction();

-- yield a single column
CREATE FUNCTION factory15(aa INT) RETURNS TABLE (aa INT) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT aa);
    END WHILE;
END;

SELECT aa FROM factory15();

DROP FUNCTION regfunction;
DROP FUNCTION factory15;
