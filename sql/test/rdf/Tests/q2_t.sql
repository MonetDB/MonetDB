--Query2:
SELECT B.prop, count(*)
	FROM triples_pso AS A,
	triples_pso AS B
	WHERE A.subj = B.subj
	AND A.prop = 17582582
	AND A.obj = 14660332
--       AND A.prop = '<type>'
--       AND A.obj = '<Text>'
	GROUP BY B.prop;
