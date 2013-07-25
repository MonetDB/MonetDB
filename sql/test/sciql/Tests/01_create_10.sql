-- create an unbounded array
CREATE ARRAY ary (x INTEGER DIMENSION[*:*:13], v FLOAT DEFAULT 3.7);

SELECT * FROM ary;

DROP ARRAY ary;

