--create a factory to make a cross product with a regular table.
CREATE TABLE myTable17 (aa INT);
INSERT INTO myTable17 VALUES (1), (2);

CREATE FUNCTION factory17() RETURNS TABLE (bb INT) BEGIN
    WHILE TRUE DO
        YIELD TABLE (SELECT 1);
        YIELD TABLE (SELECT 2);
        YIELD TABLE (SELECT 3);
    END WHILE;
END;

SELECT aa, bb FROM myTable17, factory17();
SELECT aa, bb FROM myTable17, factory17();
SELECT aa, bb FROM myTable17, factory17();

INSERT INTO myTable17 VALUES (3);

SELECT aa, bb FROM myTable17, factory17();
SELECT aa, bb FROM myTable17, factory17();
SELECT aa, bb FROM myTable17, factory17();

DROP FUNCTION factory17;

DROP TABLE myTable17;
