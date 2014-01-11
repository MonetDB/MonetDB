CREATE TABLE wtf(a int, b int, c int);

INSERT INTO wtf VALUES(1, 2, 3);
INSERT INTO wtf VALUES(4, 5, 6);

 WITH 	a AS (SELECT a, b FROM wtf WHERE a IN (1, 4)),
      	b AS (SELECT a, b, count(*) AS d FROM a GROUP BY a, b),
	c AS (SELECT a, count(*) AS e FROM b GROUP BY b) 
SELECT * FROM c;
drop table wtf;
