--
-- RANDOM
-- Test the random function
--

-- count the number of tuples originally, should be 1000
SELECT count(*) FROM onek;

-- pick three random rows, they shouldn't match
-- !! MonetDB does not allow ORDER BY or LIMIT in subqueries (and views), so disabled this test
-- (SELECT unique1 AS random
--   FROM onek ORDER BY random() LIMIT 1)
-- INTERSECT
-- (SELECT unique1 AS random
--   FROM onek ORDER BY random() LIMIT 1)
-- INTERSECT
-- (SELECT unique1 AS random
--   FROM onek ORDER BY random() LIMIT 1);

-- count roughly 1/10 of the tuples
-- this one causes an undetected error
CREATE TEMPORARY TABLE random_tbl( random double)
 ON COMMIT PRESERVE ROWS;

INSERT INTO random_tbl SELECT count(*) AS random 
  FROM onek WHERE rand() < 1.0/10;

DROP TABLE random_tbl;

CREATE TEMPORARY TABLE random_tbl( random bigint)
 ON COMMIT PRESERVE ROWS;

INSERT INTO random_tbl SELECT count(*) AS random 
  FROM onek WHERE rand() < 1.0/10;

-- select again, the count should be different
INSERT INTO RANDOM_TBL (random)
  SELECT count(*)
  FROM onek WHERE rand() < 1.0/10;

-- select again, the count should be different
INSERT INTO RANDOM_TBL (random)
  SELECT count(*)
  FROM onek WHERE rand() < 1.0/10;

-- select again, the count should be different
INSERT INTO RANDOM_TBL (random)
  SELECT count(*)
  FROM onek WHERE rand() < 1.0/10;

-- now test that they are different counts
SELECT random, count(random) FROM RANDOM_TBL
  GROUP BY random HAVING count(random) > 3;

SELECT AVG(random) FROM RANDOM_TBL
  HAVING AVG(random) NOT BETWEEN 80 AND 120;

