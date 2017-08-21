--Check the yield statement's plan is always re-executed
CREATE TABLE myTable20 (aa INT, bb CLOB, cc REAL);
INSERT INTO myTable20 VALUES (1, 'This', 1.0), (2, 'is a', 2.0), (3, 'test', 3.0);

CREATE FUNCTION factory20() RETURNS TABLE (aa INT, bb CLOB, cc REAL) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT aa, bb, cc FROM myTable20 WHERE aa = (SELECT MAX(aa) FROM myTable20));
    END WHILE;
END;

SELECT aa, bb, cc FROM factory20();

INSERT INTO myTable20 VALUES (4, 'another', 0.0);

SELECT aa, bb, cc FROM factory20();

INSERT INTO myTable20 VALUES (2, 'test', 0.0);

SELECT aa, bb, cc FROM factory20();

INSERT INTO myTable20 VALUES (8, 'to', 0.0);

SELECT aa, bb, cc FROM factory20();

INSERT INTO myTable20 VALUES (8, 'pass', 0.0);

SELECT aa, bb, cc FROM factory20();

DROP FUNCTION factory20;

DROP TABLE myTable20;
