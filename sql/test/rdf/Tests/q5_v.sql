--Query5:
SELECT B.subj, C.obj
	FROM prop14659708_pso AS A,
	prop14660306_pso AS B,
	prop17582582_pso AS C
	WHERE A.subj = B.subj
	AND A.obj = 12790038
	AND B.obj = C.subj
	AND C.obj <> 14660332;

