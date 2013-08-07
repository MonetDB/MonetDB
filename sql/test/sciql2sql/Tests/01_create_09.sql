-- create an unbounded array
CREATE ARRAY ary (x INTEGER DIMENSION[3:3:*], v FLOAT DEFAULT 3.7);
SELECT * FROM ary;
DROP ARRAY ary;

-- The actual filling is undefined, because the step size is not set.
-- The maximum would be obtained with step=1
CREATE TABLE ary (x INTEGER CHECK(x >= 3 AND x % 3=0), v FLOAT DEFAULT 3.7);
SELECT * FROM ary;
DROP TABLE ary;



