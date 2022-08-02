--Query7:
SELECT A.subj, B.obj, C.obj
	FROM triples_pso AS A,
	triples_pso AS B,
	triples_pso AS C
	WHERE A.prop = 14660087
	AND A.obj = 1856117
--       WHERE A.prop = '<Point>'
--       AND A.obj = '"end"'
	AND A.subj = B.subj
	AND B.prop = 14657240
--       AND B.prop = '<Encoding>'
	AND A.subj = C.subj
	AND C.prop = 17582582;
--       AND C.prop = '<type>';
