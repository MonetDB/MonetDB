START TRANSACTION;

-- 1. Create a test table
CREATE TABLE T1 (
  C1 INTEGER,
  C2 INTEGER,
  C3 INTEGER
);

-- 2. Insert some records
INSERT INTO T1 
VALUES (1, 2, 3),
       (1, 2, 4),
       (2, 2, 5),
       (1, 3, 6);

-- 3. Let us see which c1, c2 combination has more than one entry
-- (results are correct)
SELECT C1, C2, COUNT(*)
  FROM T1
 GROUP BY C1, C2
HAVING COUNT(*) > 1;

-- 4. Let us find all records from T1 such that C1, C2 has multiple
-- entries for a given value combination. (correct result)
SELECT T1.C1, T1.C2, T1.C3
  FROM T1,
       (
	 SELECT C1, C2
	   FROM T1
	  GROUP BY C1, C2
	 HAVING COUNT(*) > 1
       ) X
 WHERE T1.C1 = X.C1 AND T1.C2 = X.C2;

-- 5. Let us write the same logic in (4) as a subquery. (results
-- incorrect, the last row should not be there).
SELECT C1, C2, C3
  FROM T1
 WHERE (C1, C2) IN
       (
	 SELECT C1, C2
	   FROM T1
	  GROUP BY C1, C2
	 HAVING COUNT(*) > 1
       );

ROLLBACK;
