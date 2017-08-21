--Nested factories!
CREATE FUNCTION factory19a(aa int) RETURNS TABLE (aa INT) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT aa);
        SET aa = aa + 1;
        IF aa % 3 = 0 THEN
            SET aa = 0;
        END IF;
    END WHILE;
END;

CREATE FUNCTION factory19b() RETURNS TABLE (bb INT) BEGIN
    DECLARE pointer INT;
    SET pointer = 0;
    WHILE TRUE DO
        IF pointer % 2 = 0 THEN
            YIELD TABLE (SELECT cast(aa as int) FROM factory19a(1));
        ELSE
            YIELD TABLE (SELECT cast(aa as int) FROM factory19a(2));
        END IF;
        SET pointer = pointer + 1;
    END WHILE;
END;

SELECT bb FROM factory19b();
SELECT bb FROM factory19b();
SELECT bb FROM factory19b();
SELECT bb FROM factory19b();
SELECT bb FROM factory19b();
SELECT bb FROM factory19b();
SELECT bb FROM factory19b();
SELECT bb FROM factory19b();

DROP FUNCTION factory19a;
DROP FUNCTION factory19b;
