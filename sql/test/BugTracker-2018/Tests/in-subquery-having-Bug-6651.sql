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
-- incorrect, the last row (1, 3, 6) should not be there).
SELECT C1, C2, C3
  FROM T1
 WHERE (C1, C2) IN
       (
	 SELECT C1, C2
	   FROM T1
	  GROUP BY C1, C2
	 HAVING COUNT(*) > 1
       );

-- 6. Another IN query variant (swapped C1 and C2) which produces wrong results.
-- (the last row (2, 2, 5) should not be there).
SELECT C1, C2, C3
  FROM T1
 WHERE (C2, C1) IN
       (
	 SELECT C2, C1
	   FROM T1
	  GROUP BY C1, C2
	 HAVING COUNT(*) > 1
       );

-- 7. Same query as 5 but using qualified column names in subquery.
-- (produces incorrect results, same as 5)
SELECT C1, C2, C3
  FROM T1
 WHERE (C1, C2) IN
       (
	 SELECT T1.C1, T1.C2
	   FROM T1
	  GROUP BY T1.C1, T1.C2
	 HAVING COUNT(*) > 1
       );

-- 8. Same query as 5 but using alias for table and qualified column names
-- in subquery. (produces correct result, so can be used as a workaround)
SELECT C1, C2, C3
  FROM T1
 WHERE (C1, C2) IN
       (
	 SELECT T.C1, T.C2
	   FROM T1 AS T
	  GROUP BY T.C1, T.C2
	 HAVING COUNT(*) > 1
       );

-- 9. Query using NOT IN instead of IN (and change COUNT(*) = 1)
-- (produces correct result, so can be used as a workaround)
SELECT C1, C2, C3
  FROM T1
 WHERE (C1, C2) NOT IN
       (
	 SELECT C1, C2
	   FROM T1
	  GROUP BY C1, C2
	 HAVING COUNT(*) = 1
       );


-- add more data to test whether the data influences the processing
INSERT INTO T1 
VALUES (21, 22, 3),
       (21, 22, 4),
       (22, 22, 5),
       (21, 23, 6);

-- only repeat the queries which produced wrong results

-- query 5. results are incorrect, two rows (1, 3, 6) (21, 23, 6) should not be there.
SELECT C1, C2, C3
  FROM T1
 WHERE (C1, C2) IN
       (
	 SELECT C1, C2
	   FROM T1
	  GROUP BY C1, C2
	 HAVING COUNT(*) > 1
       );

-- query 6. results are incorrect, two rows (2, 2, 5) (22, 22, 5) should not be there.
SELECT C1, C2, C3
  FROM T1
 WHERE (C2, C1) IN
       (
	 SELECT C2, C1
	   FROM T1
	  GROUP BY C1, C2
	 HAVING COUNT(*) > 1
       );

-- query 7. results are incorrect, two rows (1, 3, 6) (21, 23, 6) should not be there.
SELECT C1, C2, C3
  FROM T1
 WHERE (C1, C2) IN
       (
	 SELECT T1.C1, T1.C2
	   FROM T1
	  GROUP BY T1.C1, T1.C2
	 HAVING COUNT(*) > 1
       );

ROLLBACK;
