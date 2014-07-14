--Query1:
SELECT A.obj, count(*)
	FROM triples_pso AS A
	WHERE A.prop = 17582582
--	WHERE A.prop = '<type>'
	GROUP BY A.obj;
