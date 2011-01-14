UPDATE stripes SET val = x + y; 
UPDATE stripes SET x = x+1;  -- what does it mean
							-- consider stripes with free dimensions too
UPDATE grid
SET val = CASE WHEN x > y THEN x + y  WHEN X<y THEN x - y ELSE 0 END;
UPDATE diagonal SET val = x +y; UPDATE sparse SET val = mod(rand(),16);
