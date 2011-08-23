-- create an array with <check constraint definition> on one of its cell values
CREATE ARRAY sparse( x integer DIMENSION[4], y integer DIMENSION[4], v float DEFAULT 0.0 CHECK(v>=0.0));

SELECT * FROM sparse;

DROP ARRAY sparse;

