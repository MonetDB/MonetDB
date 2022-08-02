
CREATE TABLE test (x int, y int);
insert into test (x, y) VALUES (1, 1);
insert into test (x, y) VALUES (1, 2);

SELECT *
FROM (
	    SELECT a1.x, a1.y, a2.x, a2.y
	    FROM ( SELECT * FROM test) AS a1 JOIN 
		 ( SELECT * FROM test) AS a2 
		ON a1.x = a2.x
	) AS t;

SELECT *
FROM (
	SELECT a1.x AS x1, a1.y AS y1, a2.x AS x2, a2.y AS y2
	FROM ( SELECT * FROM test) AS a1 JOIN 
	     ( SELECT * FROM test) AS a2 
	    ON a1.x = a2.x
	) AS t;

drop table test;
