--create a factory to make a cross product with a regular table
CREATE TABLE myTable17 (b INT);
INSERT INTO myTable17 (1), (2), (3);
SELECT aa, b FROM factory17(), myTable17;

CREATE FUNCTION factory17() RETURNS TABLE (aa INT) BEGIN
    YIELD TABLE (SELECT 1);
    YIELD TABLE (SELECT 2);
    YIELD TABLE (SELECT 3);
END;

SELECT aa FROM factory17();
SELECT aa FROM factory17();
SELECT aa FROM factory17();
SELECT aa FROM factory17(); --error

DROP FUNCTION factory17;

DROP TABLE myTable17;
