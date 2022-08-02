SELECT *
FROM (
	    SELECT 'apple' as fruit
	    UNION ALL SELECT 'banana'
) a
JOIN (
	    SELECT 'apple' as fruit
	    UNION ALL SELECT 'banana'
) b ON a.fruit=b.fruit
LEFT JOIN (
	    SELECT 1 as isyellow
) c ON b.fruit='banana';
