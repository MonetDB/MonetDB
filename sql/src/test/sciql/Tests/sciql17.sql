ALTER ARRAY matrix ADD r float DEFAULT sqrt( power(x,2) + power(y,2));
ALTER ARRAY matrix ADD theta float 
	DEFAULT (CASE WHEN x=0 AND y=0 THEN 0
	WHEN x> 0 THEN arcsin( CAST( x AS float) / r) 
	WHEN  x< 0 THEN -arcsin( CAST( x AS float) / r) + PI() END);
