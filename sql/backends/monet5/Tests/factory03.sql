--leaving a regular function here
CREATE FUNCTION regfunction(aa INT, bb CLOB) RETURNS TABLE (aa INT, bb CLOB) BEGIN
    RETURN TABLE (SELECT aa, bb);
END;

SELECT aa, bb FROM regfunction();

-- yield a single column
CREATE FUNCTION factory16(aa INT) RETURNS TABLE (aa INT) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT aa);
    END WHILE;
END;

SELECT aa FROM factory16();

--yield multiple columns
CREATE FUNCTION factory17(aa INT, bb CLOB) RETURNS TABLE (aa INT, bb CLOB) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT aa, bb);
    END WHILE;
END;

SELECT aa, bb FROM factory17();

--create a factory to make a cross product with a regular table
CREATE TABLE stressing (b INT);
INSERT INTO stressing (1), (2), (3);
SELECT aa, b FROM factory18(), stressing;

CREATE FUNCTION factory18() RETURNS TABLE (aa INT) BEGIN
    YIELD TABLE (SELECT 1);
    YIELD TABLE (SELECT 2);
    YIELD TABLE (SELECT 3);
END;

SELECT aa FROM factory18();
SELECT aa FROM factory18();
SELECT aa FROM factory18();
SELECT aa FROM factory18(); --error

DROP FUNCTION regfunction;
DROP FUNCTION factory16;
DROP FUNCTION factory17;
DROP FUNCTION factory18;
DROP TABLE stressing;
