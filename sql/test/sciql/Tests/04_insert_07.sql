-- An array with default cell values
CREATE ARRAY ary (x INT DIMENSION [4], y INT DIMENSION [4], v1 INT default 888, v2 INT default 999);
SELECT * FROM ary;

-- A basic INSERT stmt, should overwrite the default values
-- Does it work with all columns explicitly specified?
INSERT INTO ary(x,y,v1,v2) VALUES (0, 3, 3, 4), (1, 3, 13, 14), (2, 3, 23, 24), (3, 3, 33, 34);
SELECT * FROM ary;

-- All cells which y-dimension is 0 should get value 123 for its 'v2' column
-- Also a check that the 'v1' values of the qualified cells are not overwritten by the column default
INSERT INTO ary(v2,y) VALUES (123, 0);
SELECT * FROM ary;

-- Does it work with NULL values?
INSERT INTO ary (y, v1, v2) VALUES (0, NULL, 11), (1, 11, NULL), (2, NULL, NULL);
SELECT * FROM ary;

DROP ARRAY ary;

