--yield multiple columns
CREATE FUNCTION factory16(aa INT, bb CLOB) RETURNS TABLE (aa INT, bb CLOB) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT aa, bb);
    END WHILE;
END;

SELECT aa, bb FROM factory16();

DROP FUNCTION factory16;
