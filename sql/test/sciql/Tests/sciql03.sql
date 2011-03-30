UPDATE matrix SET v = CASE
    WHEN x>y THEN x + y
    WHEN x<y THEN x - y
    ELSE 0 END;

UPDATE diagonal SET v = x +y;

UPDATE stripes SET v = MOD(RAND(),16);

-- not in the paper
UPDATE stripes SET x = x+1;  -- what does it mean
                             -- consider stripes with free dimensions too

