CREATE ARRAY m ( x int DIMENSION[1024], v int );
UPDATE m 
	SET v = (SELECT sum(a.v * b.v) FROM a,b WHERE m.x = a.x and a.y = b.k);
