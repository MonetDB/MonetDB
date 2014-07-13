CREATE TABLE x (
x DOUBLE
)
;

INSERT INTO x VALUES (1);
INSERT INTO x VALUES (2);
INSERT INTO x VALUES (3);

-- does not return records
SELECT AVG(x) AS avgx, AVG(x) AS avgx2
FROM x;

SELECT AVG(x) AS avgx, SUM(x) AS sumx
FROM x;

SELECT AVG(x) AS avgx, COUNT(x) AS countx
FROM x;


-- fine
SELECT AVG(x) AS avgx, COUNT(*) AS countstar
FROM x;

SELECT AVG(x) AS avgx, MIN(x) AS minx
FROM x;

SELECT AVG(x) AS avgx, MAX(x) AS maxx
FROM x;

SELECT SUM(x) AS sumx, MIN(x) AS minx, MAX(x) AS maxx, COUNT(x) AS countx, COUNT(*) AS countstar
FROM x;


-- one pass standard deviation: numerically less stable than the same caluclation in R
SELECT COUNT(x) AS n1, sum(x)/count(x) as m1, sqrt( sum(x*x)/count(x) - (sum(x)/count(x))*(sum(x)/count(x)) ) as sd1
FROM x;

-- two pass standard deviation with inner count (FAILS due to the AGGREGATION BUG)
SELECT MIN(n) AS n2, MIN(m) AS m2, SQRT(SUM((x-m)*(x-m))/MIN(n)) AS sd2
FROM
(
  SELECT AVG(x) AS m, COUNT(x) AS n
  FROM x
) pass1
, x
;

-- two pass standard deviation with outer count (works in MonetDB)
SELECT COUNT(x) AS n2, MIN(m) AS m2, SQRT(SUM((x-m)*(x-m))/COUNT(x)) AS sd2
FROM
(
  SELECT AVG(x) AS m
  FROM x
) agg
, x
;

drop table x;
