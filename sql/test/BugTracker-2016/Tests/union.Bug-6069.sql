
WITH t1(x) AS (
	SELECT 0 FROM (values (0)) as v(x)), t2(x) AS (
	SELECT 0 FROM ( SELECT 0, 0, 0, 0, 0, 0, 0 UNION ALL
			SELECT 1, 0, 0, 0, 0, 0, 0) AS a23(x1, x2, x3, x4, x5, x6, x7)
	) SELECT 0 FROM t1, t2;
