--Just a more complicated test in a regular table
CREATE TABLE myTable10 (aa int, dd real);

CREATE FUNCTION factory10() RETURNS TABLE (aa int, dd real) BEGIN
    DECLARE bb INT;
    DECLARE cc REAL;
    DECLARE pointer INT;
    SET bb = 0;
    SET cc = 0;
    SET pointer = 0;
    WHILE TRUE DO
        SET bb = bb + 1;
        SET cc = cc + 1;
        INSERT INTO myTable10 VALUES (bb, cc);
        IF pointer % 2 = 0 THEN
            YIELD TABLE (SELECT MAX(aa), MIN(dd) FROM myTable10);
        ELSE
            YIELD TABLE (SELECT MIN(aa), MAX(dd) FROM myTable10);
        END IF;
        SET pointer = pointer + 1;
    END WHILE;
END;

SELECT aa, dd FROM factory10();
SELECT aa, dd FROM factory10();
SELECT aa, dd FROM factory10();
SELECT aa, dd FROM factory10();
SELECT aa, dd FROM factory10();

DROP FUNCTION factory10;

DROP TABLE myTable10;
