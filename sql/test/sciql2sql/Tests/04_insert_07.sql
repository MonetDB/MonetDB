-- An array with default cell values
--!CREATE ARRAY ary (x INT DIMENSION [4], y INT DIMENSION [4], v1 INT default 888, v2 INT default 999);
--!SELECT * FROM ary;

-- A basic INSERT stmt, should overwrite the default values
-- Does it work with all columns explicitly specified?
--!INSERT INTO ary(x,y,v1,v2) VALUES (0, 3, 3, 4), (1, 3, 13, 14), (2, 3, 23, 24), (3, 3, 33, 34);
--!SELECT * FROM ary;

-- All cells which y-dimension is 0 should get value 123 for its 'v2' column
-- Also a check that the 'v1' values of the qualified cells are not overwritten by the column default
--!INSERT INTO ary(v2,y) VALUES (123, 0);
--!SELECT * FROM ary;

-- Does it work with NULL values?
--!INSERT INTO ary (y, v1, v2) VALUES (0, NULL, 11), (1, 11, NULL), (2, NULL, NULL);
--!SELECT * FROM ary;

--!DROP ARRAY ary;

-- An array with default cell values
CREATE ARRAY ary (x INT DIMENSION [4], y INT DIMENSION [4], v1 INT default 888, v2 INT default 999);
INSERT INTO ary values
(  0, 0, 888, 999 ),
(  0, 1, 888, 999 ),
(  0, 2, 888, 999 ),
(  0, 3, 888, 999 ),

(  1, 0, 888, 999 ),
(  1, 1, 888, 999 ),
(  1, 2, 888, 999 ),
(  1, 3, 888, 999 ),

(  2, 0, 888, 999 ),
(  2, 1, 888, 999 ),
(  2, 2, 888, 999 ),
(  2, 3, 888, 999 ),

(  3, 0, 888, 999 ),
(  3, 1, 888, 999 ),
(  3, 2, 888, 999 ),
(  3, 3, 888, 999 );

SELECT * FROM ary;

-- A basic INSERT stmt, should overwrite the default values
-- Does it work with all columns explicitly specified?
INSERT INTO ary(x,y,v1,v2) VALUES (0, 3, 3, 4), (1, 3, 13, 14), (2, 3, 23, 24), (3, 3, 33, 34);
SELECT * FROM ary;
--!(  0, 0, 888, 999 ),
--!(  0, 1, 888, 999 ),
--!(  0, 2, 888, 999 ),
--!(  0, 3, 3, 4 ),
--!
--!(  1, 0, 888, 999 ),
--!(  1, 1, 888, 999 ),
--!(  1, 2, 888, 999 ),
--!(  1, 3, 13, 14 ),
--!
--!(  2, 0, 888, 999 ),
--!(  2, 1, 888, 999 ),
--!(  2, 2, 888, 999 ),
--!(  2, 3, 23, 24 ),
--!
--!(  3, 0, 888, 999 ),
--!(  3, 1, 888, 999 ),
--!(  3, 2, 888, 999 ),
--!(  3, 3, 33, 34 );

-- All cells which y-dimension is 0 should get value 123 for its 'v2' column
-- Also a check that the 'v1' values of the qualified cells are not overwritten by the column default
INSERT INTO ary(v2,y) VALUES (123, 0);
SELECT * FROM ary;
--!(  0, 0, 888, 123 ),
--!(  0, 1, 888, 999 ),
--!(  0, 2, 888, 999 ),
--!(  0, 3, 3, 4 ),
--!
--!(  1, 0, 888, 123 ),
--!(  1, 1, 888, 999 ),
--!(  1, 2, 888, 999 ),
--!(  1, 3, 13, 14 ),
--!
--!(  2, 0, 888, 123 ),
--!(  2, 1, 888, 999 ),
--!(  2, 2, 888, 999 ),
--!(  2, 3, 23, 24 ),
--!
--!(  3, 0, 888, 123 ),
--!(  3, 1, 888, 999 ),
--!(  3, 2, 888, 999 ),
--!(  3, 3, 33, 34 );


-- Does it work with NULL values?
INSERT INTO ary (y, v1, v2) VALUES (0, NULL, 11), (1, 11, NULL), (2, NULL, NULL);
SELECT * FROM ary;
--!(  0, 0, null, 11 ),
--!(  0, 1, 11, null ),
--!(  0, 2, null, null ),
--!(  0, 3, 3, 4 ),
--!
--!(  1, 0, null, 11 ),
--!(  1, 1, 11, null ),
--!(  1, 2, null, null ),
--!(  1, 3, 13, 14 ),
--!
--!(  2, 0, null, 11 ),
--!(  2, 1, 11, null ),
--!(  2, 2, null, null ),
--!(  2, 3, 23, 24 ),
--!
--!(  3, 0, null, 11 ),
--!(  3, 1, 11, null ),
--!(  3, 2, null, null ),
--!(  3, 3, 33, 34 );

DROP ARRAY ary;

