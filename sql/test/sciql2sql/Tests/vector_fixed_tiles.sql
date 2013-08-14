-- Test tiling over a 1D  fixed array
CREATE ARRAY array1D(x INTEGER DIMENSION[7], v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO array1D values 
( 0, 1, 1),
( 1, 1, 2),
( 2, 2, 1),
( 3, 2, 5),
( 4, 3, 7),
( 5, 3, 7),
( 6, 4, 1);

-- relational equivalent 
CREATE TABLE vector(x INTEGER CHECK(x >=0 and x < 7), v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO vector values 
( 0, 1, 1),
( 1, 1, 2),
( 2, 2, 1),
( 3, 2, 5),
( 4, 3, 7),
( 5, 3, 7),
( 6, 4, 1);

-- simple aggregation over a dimension 
SELECT x, count(*)
FROM array1D
GROUP BY array1D[x : 1 : x+1];

-- this can be abrevated to
SELECT x, count(*)
FROM array1D
GROUP BY array1D[x];

-- relational equivalent of both
-- every array can be looked upon as a table
SELECT x, count(*)
FROM vector
GROUP BY x;

-- a 2-size window summation
SELECT x, sum(v)
FROM array1D
GROUP BY array1D[x : 1 : x+2];

-- relational equivalent could use an arrayshift 
-- SELECT A.x, A.v+B.b FROM shift(vector,x,0) as A, shift(vector,x,1) as B
-- WHERE A.idx = B.idx
SELECT A.x, A.v+B.v 
FROM vector as A, vector as B
WHERE A.x+1 = B.x;

-- a 3-size window summarization
SELECT x, sum(v)
FROM array1D
GROUP BY array1D[x : 1 : x+3];

-- relational equivalent could use an arrayshift 
SELECT A.x, A.v+B.v+ C.v
FROM vector as A, vector as B, vector as C
WHERE A.x+1 = B.x and A.x+2 = C.x;

-- using left specified 2-windows
SELECT x, sum(v)
FROM array1D
GROUP BY array1D[x-1 : 1 : x+1];

-- relational equivalent merely shuffles the predicate
SELECT A.x, A.v+B.v 
FROM vector as A, vector as B
WHERE A.x-1 = B.x;

-- a more elaborate slices
SELECT x, sum(v)
FROM array1D
GROUP BY array1D[x : 1 : x+4];

-- alternative for simple range
SELECT A.beg, (SELECT sum(vector.v) FROM vector WHERE vector.x >= A.beg AND vector.x < A.lim)
FROM (SELECT x AS beg, x+4 AS lim FROM vector) AS A;

-- a more elaborate slices
SELECT x, sum(v)
FROM array1D
GROUP BY array1D[x : 2 : x+4];

-- alternative for simple range
SELECT A.beg, (SELECT sum(vector.v) FROM vector WHERE vector.x >= A.beg AND vector.x < A.lim AND ((vector.x - A.beg) % 2) = 0)
FROM (SELECT x AS beg, x+4 AS lim FROM vector) AS A;

-- casting a more generic predicate into relational
-- use a relative offset map to indicate group elements
-- RUNTIME ERROR, produces NULL sums !!

CREATE FUNCTION aggr(first INTEGER,step INTEGER, fin INTEGER)
RETURNS TABLE (x INTEGER, s INTEGER)
BEGIN
	CREATE LOCAL TEMPORARY TABLE pivotset(x INTEGER);
	INSERT INTO pivotset VALUES(0),(1),(2),(3);
	DECLARE pivot INTEGER;
	SET pivot = 0;
	CREATE LOCAL TEMPORARY TABLE answer(x INTEGER, s INTEGER);

	WHILE (pivot < 4) DO
		INSERT INTO answer(x,s) (SELECT pivot, sum(vector.v) FROM vector, pivotset WHERE vector.x = pivotset.x);
		UPDATE pivotset SET x=x+1;
		SET pivot = pivot+1;
	END WHILE;
	RETURN answer;
END;

SELECT * FROM aggr(0,1,4);

DROP FUNCTION aggr;
DROP ARRAY array1D;
DROP TABLE vector;

