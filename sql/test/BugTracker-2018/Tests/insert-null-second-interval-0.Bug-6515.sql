CREATE TABLE testnullsa (a boolean, b text, c tinyint, d smallint, e int, f bigint, g real, h double, i blob);
INSERT INTO testnullsa VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT a, b, c, d, e, f, g, h, i FROM testnullsa;
DROP TABLE testnullsa;

CREATE TABLE testnullsb (a date, b time, c time with time zone, d timestamp, e timestamp with time zone, f INTERVAL year to month, g INTERVAL minute to second, h decimal);
INSERT INTO testnullsb VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT a, b, c, d, e, f, g, h FROM testnullsb;
DROP TABLE testnullsb;
