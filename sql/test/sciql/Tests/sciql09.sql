-- FIXME: does this query update all 'v'-s in a row/column?
UPDATE matrix SET matrix[0:2].v = v * 1.19;

-- FIXME: the query does not match its description in the paper, nl., the CASE statement doesn't cover all value cases.
UPDATE matrix SET matrix[x].v = CASE
    WHEN matrix[x].v < 0 THEN x
	WHEN matrix[x].v >10 THEN 10 * x END;

