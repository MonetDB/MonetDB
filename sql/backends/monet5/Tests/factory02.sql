
CREATE TABLE myTable (aa int, dd real);

CREATE FUNCTION factory11() RETURNS TABLE (aa int, dd real) BEGIN
    DECLARE bb INT;
    DECLARE cc REAL;
    DECLARE pointer INT;
    SET bb = 0;
    SET cc = 0;
    SET pointer = 0;
    WHILE TRUE DO
        SET bb = bb + 1;
        SET cc = cc + 1;
        INSERT INTO myTable VALUES (bb, cc);
        IF pointer % 2 = 0 THEN
            YIELD TABLE (SELECT MAX(aa), MIN(dd) FROM myTable);
        ELSE
            YIELD TABLE (SELECT MIN(aa), MAX(dd) FROM myTable);
        END IF;
        SET pointer = pointer + 1;
    END WHILE;
END;

SELECT aa, dd FROM factory11();
SELECT aa, dd FROM factory11();
SELECT aa, dd FROM factory11();
SELECT aa, dd FROM factory11();
SELECT aa, dd FROM factory11();

CREATE FUNCTION factory12() RETURNS VARCHAR(32) BEGIN
    DECLARE aa VARCHAR(32);
    SET aa = 'This is a string! :) :) :) :) :)';
    YIELD aa;
    SET aa = SUBSTRING(aa, 0, 16);
    YIELD aa;
    SET aa = aa || aa;
    YIELD aa;
    SET aa = NULL;
    YIELD aa;
END;

SELECT factory12();
SELECT factory12();
SELECT factory12();
SELECT factory12();
SELECT factory12(); --error
SELECT factory12();
SELECT factory12();
SELECT factory12();
SELECT factory12();
SELECT factory12(); --error

CREATE FUNCTION factory13() RETURNS TABLE (aa CLOB, bb DATE) BEGIN
    YIELD TABLE (SELECT 'aa', cast('2015-01-01' AS DATE));
    YIELD TABLE (SELECT 'bb', cast('2016-02-02' AS DATE));
    YIELD TABLE (SELECT 'cc', cast('2017-03-03' AS DATE));
END;

SELECT * FROM factory13();
SELECT * FROM factory13();
SELECT * FROM factory13();

CREATE TABLE myTable2 (aa INT, bb TIME, cc CHAR(32));

CREATE FUNCTION factory14() RETURNS TABLE (aa INT, bb TIME, cc CHAR(32)) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT aa, bb, cc FROM myTable2);
    END WHILE;
END;

INSERT INTO myTable2 VALUES (1, cast('08:00:00' AS TIME), '1234');
SELECT aa, bb, cc FROM factory14();

INSERT INTO myTable2 VALUES (2, cast('09:00:00' AS TIME), '5678');
SELECT aa, bb, cc FROM factory14();

INSERT INTO myTable2 VALUES (3, cast('10:00:00' AS TIME), '91011');
SELECT aa, bb, cc FROM factory14();

CREATE FUNCTION factory15(aa INT, bb CLOB) RETURNS TABLE (aa INT, bb CLOB) BEGIN
    YIELD TABLE (SELECT aa, bb);
    SET aa = aa + 1;
    SET bb = 'just';
    YIELD TABLE (SELECT aa, bb);
    SET aa = aa + 1;
    SET bb = 'other';
    YIELD TABLE (SELECT aa, bb);
    SET aa = aa + 1;
    SET bb = 'string';
    YIELD TABLE (SELECT aa, bb);
END;

SELECT aa, bb FROM factory15(0, '');
SELECT aa, bb FROM factory15(0, '');
SELECT aa, bb FROM factory15(0, '');
SELECT aa, bb FROM factory15(0, '');
SELECT aa, bb FROM factory15(0, ''); --error

DROP FUNCTION factory11;
DROP FUNCTION factory12;
DROP FUNCTION factory13;
DROP FUNCTION factory14;
DROP FUNCTION factory15;

DROP TABLE myTable;
DROP TABLE myTable2;
