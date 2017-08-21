--create a factory to make a cross product with a regular table (two columns)
CREATE TABLE myTable18 (aa INT);
INSERT INTO myTable18 VALUES (1), (2);

CREATE FUNCTION factory18() RETURNS TABLE (bb INT) BEGIN
    YIELD TABLE (SELECT 1);
    YIELD TABLE (SELECT 2);
    YIELD TABLE (SELECT 3);
END;

SELECT aa, bb FROM myTable18, factory18();
SELECT aa, bb FROM myTable18, factory18();
SELECT aa, bb FROM myTable18, factory18();

INSERT INTO myTable18 VALUES (3);

SELECT aa, bb FROM myTable18, factory18();
SELECT aa, bb FROM myTable18, factory18();
SELECT aa, bb FROM myTable18, factory18();

DROP FUNCTION factory18;

DROP TABLE myTable18;
