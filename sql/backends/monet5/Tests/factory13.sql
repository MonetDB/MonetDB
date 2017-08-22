--Test factory function with increasing rows in the table
CREATE TABLE myTable13 (aa INT, bb TIME, cc CHAR(32));

CREATE FUNCTION factory13() RETURNS TABLE (aa INT, bb TIME, cc CHAR(32)) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT aa, bb, cc FROM myTable13);
    END WHILE;
	RETURN TABLE (SELECT aa, bb, cc FROM myTable13);
END;

INSERT INTO myTable13 VALUES (1, cast('08:00:00' AS TIME), '1234');
SELECT aa, bb, cc FROM factory13();

INSERT INTO myTable13 VALUES (2, cast('09:00:00' AS TIME), '5678');
SELECT aa, bb, cc FROM factory13();

INSERT INTO myTable13 VALUES (3, cast('10:00:00' AS TIME), '91011');
SELECT aa, bb, cc FROM factory13();

DROP FUNCTION factory13;

DROP TABLE myTable13;
