--! CREATE ARRAY stripes (x INT DIMENSION[4], y INT DIMENSION[4] CHECK(MOD(y,2) = 1), v FLOAT DEFAULT 0.0);
--! SELECT * FROM stripes;

-- not in the paper
--! UPDATE stripes SET x = x-1;  -- what does it mean: consider stripes with free dimensions too?
--! SELECT * FROM stripes;

--! DROP ARRAY stripes;

CREATE TABLE stripes (x INT CHECK(x>=0 AND x <4), y INT CHECK(y>=0 and y<4  and MOD(y,2) = 1), v FLOAT DEFAULT 0.0);

-- not in the paper
UPDATE stripes SET x = x-1;  -- what does it mean: consider stripes with free dimensions too?
SELECT * FROM stripes;

DROP TABLE stripes;

