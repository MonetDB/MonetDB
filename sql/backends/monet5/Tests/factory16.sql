--test a factory returning multiple columns
--leaving a regular function here for comparison
CREATE FUNCTION regfunction16(aa INT, bb TIMESTAMP) RETURNS TABLE (aa INT, bb TIMESTAMP) BEGIN
    RETURN TABLE (SELECT aa, bb);
END;

SELECT aa, bb FROM regfunction16(NULL, NULL);
SELECT aa, bb FROM regfunction16(1, CAST('1970-01-01 01:00:00' AS TIMESTAMP));
SELECT aa, bb FROM regfunction16(2, CAST('1970-01-01 02:00:00' AS TIMESTAMP));
SELECT aa, bb FROM regfunction16(3, CAST('1970-01-02 02:00:00' AS TIMESTAMP));

--yield multiple columns
CREATE FUNCTION factory16(aa INT, bb TIMESTAMP) RETURNS TABLE (aa INT, bb TIMESTAMP) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT NULL, NULL);
        YIELD TABLE (SELECT aa, bb);
        YIELD TABLE (SELECT aa + 1, bb + INTERVAL '1' HOUR);
        YIELD TABLE (SELECT aa + 2, bb + INTERVAL '1' HOUR + INTERVAL '1' DAY);
    END WHILE;
END;

SELECT aa, bb FROM factory16(1, CAST('1970-01-01 01:00:00' AS TIMESTAMP));
SELECT aa, bb FROM factory16(1, CAST('1970-01-01 01:00:00' AS TIMESTAMP));
SELECT aa, bb FROM factory16(1, CAST('1970-01-01 01:00:00' AS TIMESTAMP));
SELECT aa, bb FROM factory16(1, CAST('1970-01-01 01:00:00' AS TIMESTAMP));

DROP FUNCTION factory16;
