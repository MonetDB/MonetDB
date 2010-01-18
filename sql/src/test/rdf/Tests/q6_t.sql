--Query6:
SELECT A.prop, count(*)
	FROM triples_pso AS A,
	(
	 (SELECT B.subj
	  FROM triples_pso AS B
	  WHERE B.prop = 17582582
	  AND B.obj = 14660332)
--         WHERE B.prop = '<type>'
--         AND B.obj = '<Text>')
	 UNION
	 (SELECT C.subj
	  FROM triples_pso AS C,
	  triples_pso AS D
	  WHERE C.prop = 14660306
--         WHERE C.prop = '<records>'
	  AND C.obj = D.subj
	  AND D.prop = 17582582
	  AND D.obj = 14660332)
--         AND D.prop = '<type>'
--         AND D.obj = '<Text>')
	) AS uniontable
	WHERE A.subj = uniontable.subj
	GROUP BY A.prop;



