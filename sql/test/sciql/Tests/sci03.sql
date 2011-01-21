UPDATE stripes SET v = x + y; 
UPDATE stripes SET x = x+1;  -- what does it mean
							-- consider stripes with free dimensions too
UPDATE grid
SET v = CASE WHEN x > y THEN x + y  WHEN X<y THEN x - y ELSE 0 END;
UPDATE diagonal SET v = x +y;
UPDATE sparse SET v = mod(rand(),16);
