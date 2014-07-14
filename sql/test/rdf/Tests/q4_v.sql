WITH
	A(subj) as ( SELECT subj FROM prop17582582_pso WHERE obj = 14660332), 
	C(subj) as ( SELECT subj FROM prop14659603_pso WHERE obj = 17106461)
SELECT prop, obj, cnt from (
		( SELECT 'prop14657240_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657240_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14660343_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660343_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17582582_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17582582_pso as B, C
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
		( SELECT 'prop14657244_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657244_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657245_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657245_pso as B, C
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
		( SELECT 'prop14660329_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660329_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14660333_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660333_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659612_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659612_pso as B, C
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
		( SELECT 'prop14659801_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659801_pso as B, C
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
		( SELECT 'prop14659613_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659613_pso as B, C
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
		( SELECT 'prop14660254_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660254_pso as B, C
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
		( SELECT 'prop14659615_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659615_pso as B, C
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
		( SELECT 'prop14657217_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657217_pso as B, C
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
		( SELECT 'prop14660306_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660306_pso as B, C
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
		( SELECT 'prop14657208_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657208_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657221_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657221_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657249_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657249_pso as B, C
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
		( SELECT 'prop14660330_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660330_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302406_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302406_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14660321_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660321_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop11121625_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop11121625_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659714_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659714_pso as B, C
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
		( SELECT 'prop14660308_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660308_pso as B, C
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
		( SELECT 'prop14657200_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657200_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657213_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657213_pso as B, C
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
		( SELECT 'prop14657016_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657016_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302355_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302355_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302445_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302445_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302397_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302397_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302372_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302372_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302421_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302421_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302393_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302393_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657017_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657017_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657219_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657219_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302388_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302388_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302389_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302389_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302454_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302454_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302479_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302479_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302441_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302441_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302416_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302416_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302469_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302469_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302376_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302376_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302517_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302517_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302367_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302367_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302512_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302512_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659709_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659709_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14660148_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660148_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302395_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302395_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302419_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302419_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302424_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302424_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302377_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302377_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302426_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302426_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302370_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302370_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302493_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302493_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302463_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302463_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14660331_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14660331_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302457_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302457_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302481_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302481_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659715_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659715_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659532_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659532_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302436_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302436_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302468_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302468_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302413_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302413_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302516_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302516_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302478_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302478_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302361_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302361_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302371_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302371_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302362_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302362_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302363_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302363_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302386_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302386_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302390_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302390_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302430_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302430_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14657247_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657247_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302507_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302507_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302433_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302433_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302366_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302366_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302396_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302396_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302477_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302477_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302439_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302439_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302514_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302514_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302438_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302438_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302467_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302467_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302459_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302459_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302482_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302482_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302443_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302443_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302435_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302435_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14659514_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659514_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302486_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302486_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302425_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302425_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302392_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302392_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302473_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302473_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302455_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302455_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302434_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302434_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302492_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302492_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302437_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302437_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302501_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302501_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302490_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302490_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302460_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302460_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302519_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302519_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302398_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302398_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302518_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302518_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302495_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302495_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302489_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302489_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302364_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302364_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302474_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302474_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302431_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302431_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302496_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302496_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302440_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302440_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302378_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302378_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302358_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302358_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302369_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302369_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302511_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302511_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302442_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302442_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302497_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302497_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302500_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302500_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302465_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302465_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302384_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302384_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302427_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302427_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302379_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302379_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302387_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302387_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302466_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302466_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302503_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302503_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302498_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302498_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302381_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302381_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302504_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302504_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302383_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302383_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302403_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302403_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302356_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302356_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302428_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302428_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302357_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302357_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302359_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302359_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302485_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302485_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302404_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302404_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302429_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302429_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302368_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302368_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302470_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302470_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302483_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302483_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302391_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302391_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302385_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302385_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302484_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302484_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302417_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302417_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302374_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302374_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302447_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302447_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302521_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302521_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302360_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302360_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302456_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302456_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302382_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302382_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302462_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302462_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302449_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302449_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302451_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302451_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302450_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302450_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302420_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302420_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302410_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302410_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302380_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302380_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302422_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302422_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302423_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302423_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302444_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302444_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302448_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302448_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302375_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302375_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302400_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302400_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302402_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302402_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302418_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302418_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302515_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302515_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302505_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302505_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302399_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302399_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302491_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302491_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302476_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302476_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302446_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302446_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302458_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302458_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302412_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302412_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302464_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302464_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302520_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302520_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302499_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302499_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302401_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302401_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302453_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302453_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302513_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302513_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302432_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302432_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302409_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302409_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302506_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302506_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302461_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302461_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302452_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302452_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302411_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302411_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302373_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302373_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302494_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302494_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302472_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302472_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302475_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302475_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302394_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302394_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302480_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302480_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302408_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302408_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302487_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302487_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302415_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302415_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302488_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302488_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302508_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302508_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302509_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302509_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302510_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302510_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302471_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302471_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302502_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302502_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302414_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302414_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302405_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302405_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302407_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302407_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop17302365_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop17302365_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
	 UNION ALL
		( SELECT 'prop14742688_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14742688_pso as B, C
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
		( SELECT 'prop14657018_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657018_pso as B, C
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
		( SELECT 'prop14657019_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14657019_pso as B, C
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
		( SELECT 'prop14659605_pso' as prop, B.obj, count(*) as cnt
			FROM A, prop14659605_pso as B, C
			WHERE A.subj = B.subj AND C.subj = B.subj
			GROUP BY B.obj HAVING count(*) > 1
		)
 ) as trip;
