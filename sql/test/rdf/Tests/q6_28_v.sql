WITH uniontable(subj) as( 
		( SELECT B.subj FROM prop17582582_pso AS B WHERE B.obj = 14660332)
	UNION
		( SELECT C.subj FROM prop14660306_pso AS C, prop17582582_pso AS D WHERE C.obj = D.subj AND D.obj = 14660332 ))
SELECT * from (
		(SELECT 'prop14657017_pso', count(*)
			FROM prop14657017_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657018_pso', count(*)
			FROM prop14657018_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657019_pso', count(*)
			FROM prop14657019_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657202_pso', count(*)
			FROM prop14657202_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657206_pso', count(*)
			FROM prop14657206_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657207_pso', count(*)
			FROM prop14657207_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657210_pso', count(*)
			FROM prop14657210_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657214_pso', count(*)
			FROM prop14657214_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657215_pso', count(*)
			FROM prop14657215_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657217_pso', count(*)
			FROM prop14657217_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657220_pso', count(*)
			FROM prop14657220_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657222_pso', count(*)
			FROM prop14657222_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657234_pso', count(*)
			FROM prop14657234_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657240_pso', count(*)
			FROM prop14657240_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657244_pso', count(*)
			FROM prop14657244_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657248_pso', count(*)
			FROM prop14657248_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659533_pso', count(*)
			FROM prop14659533_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659603_pso', count(*)
			FROM prop14659603_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659613_pso', count(*)
			FROM prop14659613_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659708_pso', count(*)
			FROM prop14659708_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659801_pso', count(*)
			FROM prop14659801_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659802_pso', count(*)
			FROM prop14659802_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659939_pso', count(*)
			FROM prop14659939_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660087_pso', count(*)
			FROM prop14660087_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660290_pso', count(*)
			FROM prop14660290_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660306_pso', count(*)
			FROM prop14660306_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660329_pso', count(*)
			FROM prop14660329_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17582582_pso', count(*)
			FROM prop17582582_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
 ) as trip;
