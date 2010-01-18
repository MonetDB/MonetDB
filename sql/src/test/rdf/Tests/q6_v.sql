WITH uniontable(subj) as( 
		( SELECT B.subj FROM prop17582582_pso AS B WHERE B.obj = 14660332)
	UNION 
		( SELECT C.subj FROM prop14660306_pso AS C, prop17582582_pso AS D WHERE C.obj = D.subj AND D.obj = 14660332 ))
SELECT * from (
		(SELECT 'prop14657240_pso', count(*)
			FROM prop14657240_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660343_pso', count(*)
			FROM prop14660343_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17582582_pso', count(*)
			FROM prop17582582_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657202_pso', count(*)
			FROM prop14657202_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657244_pso', count(*)
			FROM prop14657244_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657245_pso', count(*)
			FROM prop14657245_pso as A, uniontable
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
		(SELECT 'prop14660329_pso', count(*)
			FROM prop14660329_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660333_pso', count(*)
			FROM prop14660333_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659612_pso', count(*)
			FROM prop14659612_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659802_pso', count(*)
			FROM prop14659802_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659801_pso', count(*)
			FROM prop14659801_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657234_pso', count(*)
			FROM prop14657234_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659613_pso', count(*)
			FROM prop14659613_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659939_pso', count(*)
			FROM prop14659939_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660254_pso', count(*)
			FROM prop14660254_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659603_pso', count(*)
			FROM prop14659603_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659615_pso', count(*)
			FROM prop14659615_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657207_pso', count(*)
			FROM prop14657207_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657217_pso', count(*)
			FROM prop14657217_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659708_pso', count(*)
			FROM prop14659708_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660306_pso', count(*)
			FROM prop14660306_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657206_pso', count(*)
			FROM prop14657206_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657208_pso', count(*)
			FROM prop14657208_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657221_pso', count(*)
			FROM prop14657221_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657249_pso', count(*)
			FROM prop14657249_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659533_pso', count(*)
			FROM prop14659533_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660330_pso', count(*)
			FROM prop14660330_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302406_pso', count(*)
			FROM prop17302406_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660321_pso', count(*)
			FROM prop14660321_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop11121625_pso', count(*)
			FROM prop11121625_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659714_pso', count(*)
			FROM prop14659714_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657215_pso', count(*)
			FROM prop14657215_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660308_pso', count(*)
			FROM prop14660308_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657214_pso', count(*)
			FROM prop14657214_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657200_pso', count(*)
			FROM prop14657200_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657213_pso', count(*)
			FROM prop14657213_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657220_pso', count(*)
			FROM prop14657220_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657016_pso', count(*)
			FROM prop14657016_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302355_pso', count(*)
			FROM prop17302355_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302445_pso', count(*)
			FROM prop17302445_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302397_pso', count(*)
			FROM prop17302397_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302372_pso', count(*)
			FROM prop17302372_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302421_pso', count(*)
			FROM prop17302421_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302393_pso', count(*)
			FROM prop17302393_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657017_pso', count(*)
			FROM prop14657017_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657219_pso', count(*)
			FROM prop14657219_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302388_pso', count(*)
			FROM prop17302388_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302389_pso', count(*)
			FROM prop17302389_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302454_pso', count(*)
			FROM prop17302454_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302479_pso', count(*)
			FROM prop17302479_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302441_pso', count(*)
			FROM prop17302441_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302416_pso', count(*)
			FROM prop17302416_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302469_pso', count(*)
			FROM prop17302469_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302376_pso', count(*)
			FROM prop17302376_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302517_pso', count(*)
			FROM prop17302517_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302367_pso', count(*)
			FROM prop17302367_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302512_pso', count(*)
			FROM prop17302512_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659709_pso', count(*)
			FROM prop14659709_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660148_pso', count(*)
			FROM prop14660148_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302395_pso', count(*)
			FROM prop17302395_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302419_pso', count(*)
			FROM prop17302419_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302424_pso', count(*)
			FROM prop17302424_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302377_pso', count(*)
			FROM prop17302377_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302426_pso', count(*)
			FROM prop17302426_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302370_pso', count(*)
			FROM prop17302370_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302493_pso', count(*)
			FROM prop17302493_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302463_pso', count(*)
			FROM prop17302463_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14660331_pso', count(*)
			FROM prop14660331_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302457_pso', count(*)
			FROM prop17302457_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302481_pso', count(*)
			FROM prop17302481_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659715_pso', count(*)
			FROM prop14659715_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659532_pso', count(*)
			FROM prop14659532_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302436_pso', count(*)
			FROM prop17302436_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302468_pso', count(*)
			FROM prop17302468_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302413_pso', count(*)
			FROM prop17302413_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302516_pso', count(*)
			FROM prop17302516_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302478_pso', count(*)
			FROM prop17302478_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302361_pso', count(*)
			FROM prop17302361_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302371_pso', count(*)
			FROM prop17302371_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302362_pso', count(*)
			FROM prop17302362_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302363_pso', count(*)
			FROM prop17302363_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302386_pso', count(*)
			FROM prop17302386_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302390_pso', count(*)
			FROM prop17302390_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302430_pso', count(*)
			FROM prop17302430_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657247_pso', count(*)
			FROM prop14657247_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302507_pso', count(*)
			FROM prop17302507_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302433_pso', count(*)
			FROM prop17302433_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302366_pso', count(*)
			FROM prop17302366_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302396_pso', count(*)
			FROM prop17302396_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302477_pso', count(*)
			FROM prop17302477_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302439_pso', count(*)
			FROM prop17302439_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302514_pso', count(*)
			FROM prop17302514_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302438_pso', count(*)
			FROM prop17302438_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302467_pso', count(*)
			FROM prop17302467_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302459_pso', count(*)
			FROM prop17302459_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302482_pso', count(*)
			FROM prop17302482_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302443_pso', count(*)
			FROM prop17302443_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302435_pso', count(*)
			FROM prop17302435_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659514_pso', count(*)
			FROM prop14659514_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302486_pso', count(*)
			FROM prop17302486_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302425_pso', count(*)
			FROM prop17302425_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302392_pso', count(*)
			FROM prop17302392_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302473_pso', count(*)
			FROM prop17302473_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302455_pso', count(*)
			FROM prop17302455_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302434_pso', count(*)
			FROM prop17302434_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302492_pso', count(*)
			FROM prop17302492_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302437_pso', count(*)
			FROM prop17302437_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302501_pso', count(*)
			FROM prop17302501_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302490_pso', count(*)
			FROM prop17302490_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302460_pso', count(*)
			FROM prop17302460_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302519_pso', count(*)
			FROM prop17302519_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302398_pso', count(*)
			FROM prop17302398_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302518_pso', count(*)
			FROM prop17302518_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302495_pso', count(*)
			FROM prop17302495_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302489_pso', count(*)
			FROM prop17302489_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302364_pso', count(*)
			FROM prop17302364_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302474_pso', count(*)
			FROM prop17302474_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302431_pso', count(*)
			FROM prop17302431_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302496_pso', count(*)
			FROM prop17302496_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302440_pso', count(*)
			FROM prop17302440_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302378_pso', count(*)
			FROM prop17302378_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302358_pso', count(*)
			FROM prop17302358_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302369_pso', count(*)
			FROM prop17302369_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302511_pso', count(*)
			FROM prop17302511_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302442_pso', count(*)
			FROM prop17302442_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302497_pso', count(*)
			FROM prop17302497_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302500_pso', count(*)
			FROM prop17302500_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302465_pso', count(*)
			FROM prop17302465_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302384_pso', count(*)
			FROM prop17302384_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302427_pso', count(*)
			FROM prop17302427_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302379_pso', count(*)
			FROM prop17302379_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302387_pso', count(*)
			FROM prop17302387_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302466_pso', count(*)
			FROM prop17302466_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302503_pso', count(*)
			FROM prop17302503_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302498_pso', count(*)
			FROM prop17302498_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302381_pso', count(*)
			FROM prop17302381_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302504_pso', count(*)
			FROM prop17302504_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302383_pso', count(*)
			FROM prop17302383_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302403_pso', count(*)
			FROM prop17302403_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302356_pso', count(*)
			FROM prop17302356_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302428_pso', count(*)
			FROM prop17302428_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302357_pso', count(*)
			FROM prop17302357_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302359_pso', count(*)
			FROM prop17302359_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302485_pso', count(*)
			FROM prop17302485_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302404_pso', count(*)
			FROM prop17302404_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302429_pso', count(*)
			FROM prop17302429_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302368_pso', count(*)
			FROM prop17302368_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302470_pso', count(*)
			FROM prop17302470_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302483_pso', count(*)
			FROM prop17302483_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302391_pso', count(*)
			FROM prop17302391_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302385_pso', count(*)
			FROM prop17302385_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302484_pso', count(*)
			FROM prop17302484_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302417_pso', count(*)
			FROM prop17302417_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302374_pso', count(*)
			FROM prop17302374_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302447_pso', count(*)
			FROM prop17302447_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302521_pso', count(*)
			FROM prop17302521_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302360_pso', count(*)
			FROM prop17302360_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302456_pso', count(*)
			FROM prop17302456_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302382_pso', count(*)
			FROM prop17302382_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302462_pso', count(*)
			FROM prop17302462_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302449_pso', count(*)
			FROM prop17302449_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302451_pso', count(*)
			FROM prop17302451_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302450_pso', count(*)
			FROM prop17302450_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302420_pso', count(*)
			FROM prop17302420_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302410_pso', count(*)
			FROM prop17302410_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302380_pso', count(*)
			FROM prop17302380_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302422_pso', count(*)
			FROM prop17302422_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302423_pso', count(*)
			FROM prop17302423_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302444_pso', count(*)
			FROM prop17302444_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302448_pso', count(*)
			FROM prop17302448_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302375_pso', count(*)
			FROM prop17302375_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302400_pso', count(*)
			FROM prop17302400_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302402_pso', count(*)
			FROM prop17302402_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302418_pso', count(*)
			FROM prop17302418_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302515_pso', count(*)
			FROM prop17302515_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302505_pso', count(*)
			FROM prop17302505_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302399_pso', count(*)
			FROM prop17302399_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302491_pso', count(*)
			FROM prop17302491_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302476_pso', count(*)
			FROM prop17302476_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302446_pso', count(*)
			FROM prop17302446_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302458_pso', count(*)
			FROM prop17302458_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302412_pso', count(*)
			FROM prop17302412_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302464_pso', count(*)
			FROM prop17302464_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302520_pso', count(*)
			FROM prop17302520_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302499_pso', count(*)
			FROM prop17302499_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302401_pso', count(*)
			FROM prop17302401_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302453_pso', count(*)
			FROM prop17302453_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302513_pso', count(*)
			FROM prop17302513_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302432_pso', count(*)
			FROM prop17302432_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302409_pso', count(*)
			FROM prop17302409_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302506_pso', count(*)
			FROM prop17302506_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302461_pso', count(*)
			FROM prop17302461_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302452_pso', count(*)
			FROM prop17302452_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302411_pso', count(*)
			FROM prop17302411_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302373_pso', count(*)
			FROM prop17302373_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302494_pso', count(*)
			FROM prop17302494_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302472_pso', count(*)
			FROM prop17302472_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302475_pso', count(*)
			FROM prop17302475_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302394_pso', count(*)
			FROM prop17302394_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302480_pso', count(*)
			FROM prop17302480_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302408_pso', count(*)
			FROM prop17302408_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302487_pso', count(*)
			FROM prop17302487_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302415_pso', count(*)
			FROM prop17302415_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302488_pso', count(*)
			FROM prop17302488_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302508_pso', count(*)
			FROM prop17302508_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302509_pso', count(*)
			FROM prop17302509_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302510_pso', count(*)
			FROM prop17302510_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302471_pso', count(*)
			FROM prop17302471_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302502_pso', count(*)
			FROM prop17302502_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302414_pso', count(*)
			FROM prop17302414_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302405_pso', count(*)
			FROM prop17302405_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302407_pso', count(*)
			FROM prop17302407_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop17302365_pso', count(*)
			FROM prop17302365_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14742688_pso', count(*)
			FROM prop14742688_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657248_pso', count(*)
			FROM prop14657248_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657018_pso', count(*)
			FROM prop14657018_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657222_pso', count(*)
			FROM prop14657222_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657019_pso', count(*)
			FROM prop14657019_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14657210_pso', count(*)
			FROM prop14657210_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
	 UNION ALL
		(SELECT 'prop14659605_pso', count(*)
			FROM prop14659605_pso as A, uniontable
			WHERE A.subj = uniontable.subj
		)
 ) as trip;
