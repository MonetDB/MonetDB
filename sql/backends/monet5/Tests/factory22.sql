--Factories with aggregation functions

CREATE FUNCTION factory22(entry INT) RETURNS REAL BEGIN
    WHILE TRUE DO
        YIELD SELECT entry - 5;
    END WHILE;
END;

CREATE TABLE myTable22 (aa INT);

INSERT INTO myTable22 VALUES (1), (2), (3), (4), (1), (3), (2), (8);

SELECT AVG(factory22(myTable22.aa)) FROM myTable22;

DROP FUNCTION factory22;

DROP TABLE myTable22;
