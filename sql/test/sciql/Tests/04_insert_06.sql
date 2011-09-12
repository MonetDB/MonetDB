-- An array without default cell values
CREATE ARRAY ary (x INT DIMENSION [4], y INT DIMENSION [4], v1 INT, v2 INT);
SELECT * FROM ary;

-- A basic INSERT stmt, should overwrite the NULL values
INSERT INTO ary VALUES (1, 0, 10, 11), (1, 1, 11, 12), (1, 2, 12, 13), (1, 3, 13, 14);
SELECT * FROM ary;

-- Does it work with all columns explicitly specified?
INSERT INTO ary(x,y,v1,v2) VALUES (0, 3, 3, 4), (1, 3, 13, 14), (2, 3, 23, 24), (3, 3, 33, 34);
SELECT * FROM ary;

-- All cells which y-dimension is 0 should get value 123 for its 'v2' column
-- Also a check if it works with columns given in arbitrary order
INSERT INTO ary(v2,y) VALUES (123, 0);
SELECT * FROM ary;

-- All cells which y-dimension is 0 should get value 321 for its 'v2' column
-- This is to check if the second set of values indeed overwrite the first set of values in the same query
INSERT INTO ary(v2,y) VALUES (123, 1), (321, 1);
SELECT * FROM ary;

-- Does it work with NULL values?
INSERT INTO ary VALUES (1, 0, NULL, 11), (1, 1, 11, NULL), (1, 2, 12, NULL), (1, 3, NULL, 14);
SELECT * FROM ary;

DROP ARRAY ary;

