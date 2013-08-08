-- An array without default cell values
--! CREATE ARRAY ary (x INT DIMENSION [4], y INT DIMENSION [4], v1 INT, v2 INT);
--! SELECT * FROM ary;

-- A basic INSERT stmt, should overwrite the NULL values
--! INSERT INTO ary VALUES (1, 0, 10, 11), (1, 1, 11, 12), (1, 2, 12, 13), (1, 3, 13, 14);
--! SELECT * FROM ary;

-- Does it work with all columns explicitly specified?
--! INSERT INTO ary(x,y,v1,v2) VALUES (0, 3, 3, 4), (1, 3, 13, 14), (2, 3, 23, 24), (3, 3, 33, 34);
--! SELECT * FROM ary;

-- All cells which y-dimension is 0 should get value 123 for its 'v2' column
-- Also a check if it works with columns given in arbitrary order
--! INSERT INTO ary(v2,y) VALUES (123, 0);
--! SELECT * FROM ary;

-- All cells which y-dimension is 0 should get value 321 for its 'v2' column
-- This is to check if the second set of values indeed overwrite the first set of values in the same query
--! INSERT INTO ary(v2,y) VALUES (123, 1), (321, 1);
--! SELECT * FROM ary;

-- Does it work with NULL values?
--! INSERT INTO ary VALUES (1, 0, NULL, 11), (1, 1, 11, NULL), (1, 2, 12, NULL), (1, 3, NULL, 14);
--! SELECT * FROM ary;

--! DROP ARRAY ary;

-- An array without default cell values
CREATE TABLE ary (x INT CHECK(x>=0 and x <4), y INT (y>=0 and y<4), v1 INT, v2 INT);
INSERT INTO ary values
(  0, 0, null, null),
(  0, 1, null, null),
(  0, 2, null, null),
(  0, 3, null, null),

(  1, 0, null, null),
(  1, 1, null, null),
(  1, 2, null, null),
(  1, 3, null, null),

(  2, 0, null, null),
(  2, 1, null, null),
(  2, 2, null, null),
(  2, 3, null, null),

(  3, 0, null, null),
(  3, 1, null, null),
(  3, 2, null, null),
(  3, 3, null, null);

SELECT * FROM ary;

-- A basic INSERT stmt, should overwrite the NULL values
INSERT INTO ary VALUES (1, 0, 10, 11), (1, 1, 11, 12), (1, 2, 12, 13), (1, 3, 13, 14);
SELECT * FROM ary;
--!(  0, 0, null, null),
--!(  0, 1, null, null),
--!(  0, 2, null, null),
--!(  0, 3, null, null),
--!
--!(  1, 0, 10, 11),
--!(  1, 1, 11, 12),
--!(  1, 2, 12, 13),
--!(  1, 3, 13, 14),
--!
--!(  2, 0, null, null),
--!(  2, 1, null, null),
--!(  2, 2, null, null),
--!(  2, 3, null, null),
--!
--!(  3, 0, null, null),
--!(  3, 1, null, null),
--!(  3, 2, null, null),
--!(  3, 3, null, null);

-- Does it work with all columns explicitly specified?
INSERT INTO ary(x,y,v1,v2) VALUES (0, 3, 3, 4), (1, 3, 13, 14), (2, 3, 23, 24), (3, 3, 33, 34);
SELECT * FROM ary;
--!(  0, 0, null, null),
--!(  0, 1, null, null),
--!(  0, 2, null, null),
--!(  0, 3, 3, 4),
--!
--!(  1, 0, 10, 11),
--!(  1, 1, 11, 12),
--!(  1, 2, 12, 13),
--!(  1, 3, 13, 14),
--!
--!(  2, 0, null, null),
--!(  2, 1, null, null),
--!(  2, 2, null, null),
--!(  2, 3, 23, 24),
--!
--!(  3, 0, null, null),
--!(  3, 1, null, null),
--!(  3, 2, null, null),
--!(  3, 3, 33, 34);

-- All cells which y-dimension is 0 should get value 123 for its 'v2' column
-- Also a check if it works with columns given in arbitrary order
INSERT INTO ary(v2,y) VALUES (123, 0);
SELECT * FROM ary;
--!(  0, 0, null, 123),
--!(  0, 1, null, null),
--!(  0, 2, null, null),
--!(  0, 3, 3, 4),
--!
--!(  1, 0, 10, 123),
--!(  1, 1, 11, 12),
--!(  1, 2, 12, 13),
--!(  1, 3, 13, 14),
--!
--!(  2, 0, null, 123),
--!(  2, 1, null, null),
--!(  2, 2, null, null),
--!(  2, 3, 23, 24),
--!
--!(  3, 0, null, 123),
--!(  3, 1, null, null),
--!(  3, 2, null, null),
--!(  3, 3, 33, 34);

-- All cells which y-dimension is 0 should get value 321 for its 'v2' column
-- This is to check if the second set of values indeed overwrite the first set of values in the same query
INSERT INTO ary(v2,y) VALUES (123, 1), (321, 1);
SELECT * FROM ary;
--!(  0, 0, null, 123),
--!(  0, 1, null, 123),
--!(  0, 2, null, null),
--!(  0, 3, 3, 4),
--!
--!(  1, 0, 10, 123),
--!(  1, 1, 11, 123),
--!(  1, 2, 12, 13),
--!(  1, 3, 13, 14),
--!
--!(  2, 0, null, 123),
--!(  2, 1, null, 123),
--!(  2, 2, null, null),
--!(  2, 3, 23, 24),
--!
--!(  3, 0, null, 123),
--!(  3, 1, null, 123),
--!(  3, 2, null, null),
--!(  3, 3, 33, 34);

-- Does it work with NULL values?
INSERT INTO ary VALUES (1, 0, NULL, 11), (1, 1, 11, NULL), (1, 2, 12, NULL), (1, 3, NULL, 14);
SELECT * FROM ary;
--!(  0, 0, null, 123),
--!(  0, 1, null, 123),
--!(  0, 2, null, null),
--!(  0, 3, 3, 4),
--!
--!(  1, 0, null, 11),
--!(  1, 1, 11, null),
--!(  1, 2, 12, null),
--!(  1, 3, 13, null),
--!
--!(  2, 0, null, 123),
--!(  2, 1, null, 123),
--!(  2, 2, null, null),
--!(  2, 3, 23, 24),
--!
--!(  3, 0, null, 123),
--!(  3, 1, null, 123),
--!(  3, 2, null, null),
--!(  3, 3, 33, 34);

DROP TABLE ary;

