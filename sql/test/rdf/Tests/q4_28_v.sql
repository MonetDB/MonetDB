WITH
	A(subj) as ( SELECT subj FROM prop17582582_pso WHERE obj = 14660332), 
	C(subj) as ( SELECT subj FROM prop14659603_pso WHERE obj = 17106461)
SELECT prop, obj, cnt from (
		( SELECT 'prop14657017_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657017_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657018_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657018_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657019_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657019_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657202_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657202_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657206_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657206_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657207_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657207_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657210_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657210_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657214_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657214_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657215_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657215_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657217_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657217_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657220_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657220_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657222_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657222_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657234_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657234_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657240_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657240_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657244_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657244_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657248_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657248_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659533_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659533_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659603_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659603_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659613_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659613_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659708_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659708_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659801_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659801_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659802_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659802_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659939_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659939_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14660087_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660087_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14660290_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660290_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14660306_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660306_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14660329_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660329_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17582582_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17582582_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
 ) as trip;
