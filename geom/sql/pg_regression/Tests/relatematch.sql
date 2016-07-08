SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT '101202FFF' as a, 'TTTTTTFFF' as b) as f;
SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT '101202FFF' as a, 'T0T2TTFFF' as b) as f;
SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT '101202FFF' as a, '101202FFF' as b) as f;
SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT '101202FFF' as a, '101102FFF' as b) as f;
SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT 'FFFFFFFFF' as a, '1FFFFFFFF' as b) as f;
SELECT a, b, ST_RelateMatch(a,b) FROM
	( SELECT 'FFFFFFFFF' as a, '*FFFFFFFF' as b) as f;
