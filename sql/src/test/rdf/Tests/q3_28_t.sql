--Query3:
SELECT B.prop, B.obj, count(*)
	FROM triples_pso AS A,
	triples_pso AS B
	WHERE A.subj = B.subj
	AND A.prop = 17582582
	AND A.obj = 14660332
--       AND A.prop = '<type>'
--       AND A.obj = '<Text>'
	AND B.prop in (
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
	GROUP BY B.prop, B.obj
	HAVING count(*) > 1;



