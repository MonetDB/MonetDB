SET vector[0:2].v = (expr1,expr2); 
SET vector[x].v = CASE  WHEN vector[x].v < 0  THEN 0 
	WHEN vector[x].v >10 THEN 10 * x END ;

	
