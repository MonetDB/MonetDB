--Query5:
SELECT B.subj, C.obj
	FROM triples_pso AS A,
	triples_pso AS B,
	triples_pso AS C
	WHERE A.subj = B.subj
	AND A.prop = 14659708
	AND A.obj = 12790038
	AND B.prop = 14660306
--       AND A.prop = '<origin>'
--       AND A.obj = '<info:marcorg/DLC>'
--       AND B.prop = '<records>'
	AND B.obj = C.subj
	AND C.prop = 17582582
	AND C.obj <> 14660332;
--       AND C.prop = '<type>'
--       AND C.obj != '<Text>';



