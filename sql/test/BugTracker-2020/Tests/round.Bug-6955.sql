CREATE TABLE decimals(d DECIMAL(4,1), prec INTEGER);
INSERT INTO decimals VALUES ('999.9', 0);
SELECT ROUND(d, prec) FROM decimals;
-- expected: 1000, actual: 100
SELECT ROUND(CAST(999.9 AS DECIMAL(4,1)), 0);
-- correct result: 1000
SELECT ROUND(d, 0) FROM decimals;
-- correct result: 1000.0
SELECT ROUND(d, 0.1) FROM decimals;
-- expected: error or 1000.0, got 100.0
SELECT ROUND(CAST(999.9 AS DECIMAL(4,1)), 0.1);
-- same result: got 100.0
drop table decimals;
