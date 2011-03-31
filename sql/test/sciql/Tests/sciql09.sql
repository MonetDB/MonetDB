-- FIXME: do these queries update all 'v'-s in a row/column?
UPDATE matrix SET matrix[0:2].v = v * 1.19;

UPDATE matrix SET matrix[x].v = CASE
    WHEN matrix[x].v < 0 THEN x
	WHEN matrix[x].v >10 THEN 10 * x 
	ELSE 0 END;

