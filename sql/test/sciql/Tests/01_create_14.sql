-- create an array with <check constraint definition> on one of its dimensions
CREATE ARRAY stripes (x INT DIMENSION[4], y INT DIMENSION[4] CHECK(MOD(y,2) = 1), v FLOAT DEFAULT 0.0);

SELECT * FROM stripes;

DROP ARRAY stripes;

