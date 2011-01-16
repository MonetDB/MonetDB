--Query4:
SELECT B.prop, B.obj, count(*)
	FROM triples_pso AS A,
	triples_pso AS B,
	triples_pso AS C
	WHERE A.subj = B.subj
	AND A.prop = 17582582
	AND A.obj = 14660332
--       AND A.prop = '<type>'
--       AND A.obj = '<Text>'
	AND C.subj = B.subj
	AND C.prop = 14659603
	AND C.obj = 17106461
--       AND C.prop = '<language>'
--       AND C.obj =  '<language/iso639-2b/fre>'
	GROUP BY B.prop, B.obj
	HAVING count(*) > 1;

