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
	AND A.prop in (
		14657017,
		14657018,
		14657019,
		14657202,
		14657206,
		14657207,
		14657210,
		14657214,
		14657215,
		14657217,
		14657220,
		14657222,
		14657234,
		14657240,
		14657244,
		14657248,
		14659533,
		14659603,
		14659613,
		14659708,
		14659801,
		14659802,
		14659939,
		14660087,
		14660290,
		14660306,
		14660329,
		17582582)
	GROUP BY A.prop;



