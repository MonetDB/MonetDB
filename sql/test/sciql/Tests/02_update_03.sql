CREATE ARRAY stripes (x INT DIMENSION[4], y INT DIMENSION[4] CHECK(MOD(y,2) = 1), v FLOAT DEFAULT 0.0);
SELECT * FROM stripes;
-- not in the paper
UPDATE stripes SET x = x-1;  -- what does it mean: consider stripes with free dimensions too?
SELECT * FROM stripes;
DROP ARRAY stripes;

