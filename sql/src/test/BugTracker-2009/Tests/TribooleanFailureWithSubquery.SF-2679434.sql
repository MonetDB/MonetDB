-- I found a couple of inconsistencies (guess bugs) around tri-boolean evaluations (involving NULL, or more precisely UNKNOWN).

-- 1) NULL arithmetically compared should always give NULL, and in bi-boolean decisions like WHERE or HAVING conditions NULL should always be mapped to FALSE, i.e. dropping the rows. At least in combination with subquerying this is not the case.

-- 2) NULL logically compared should be propagated using tri-boolean logic. It is not. 

-------------------------------
-- on 1), arithmetic comparison
-------------------------------

-- correct
SELECT SUM(a) AS suma
FROM ( 
  SELECT *
  FROM ( 
    SELECT 1 AS a, 'x' AS b
    ) sub
  WHERE b IN ('y')
) sub2
;

-- correct
SELECT SUM(a) AS suma
FROM ( 
  SELECT *
  FROM ( 
    SELECT 1 AS a, 'x' AS b
    ) sub
  WHERE b IN ('y')
) sub2
HAVING SUM(a) IS NULL
;

-- correct
SELECT SUM(a) AS suma
FROM ( 
  SELECT *
  FROM ( 
    SELECT 1 AS a, 'x' AS b
    ) sub
  WHERE b IN ('y')
) sub2
HAVING SUM(a) IS NOT NULL
;

-- WRONG
SELECT SUM(a) AS suma
FROM ( 
  SELECT *
  FROM ( 
    SELECT 1 AS a, 'x' AS b
    ) sub
  WHERE b IN ('y')
) sub2
HAVING SUM(a) > 0
;

-- STILL WRONG
SELECT * 
FROM (
  SELECT SUM(a) AS suma, NULL AS mynull
  FROM ( 
    SELECT *
    FROM ( 
      SELECT 1 AS a, 'x' AS b
      ) sub
    WHERE b IN ('y')
  ) sub2
) sub3
WHERE suma > 0
;

-- although this works
SELECT *
FROM (
  SELECT SUM(a) AS suma, NULL AS mynull
  FROM ( 
    SELECT *
    FROM ( 
      SELECT 1 AS a, 'x' AS b
      ) sub
    WHERE b IN ('y')
  ) sub2
) sub3
WHERE mynull > 0
;

-- WRONG
SELECT * 
FROM (
  SELECT 1 AS a, NULL AS b
) sub
WHERE b>0
;

-- aparently right
SELECT * 
FROM (
  SELECT 1 AS a, NULL AS b
) sub
WHERE NOT b<=0
;


-- LOOKS better when using table instead of subquery, BUT
CREATE TABLE dummy (a INTEGER, b INTEGER);
INSERT INTO dummy VALUES (1, NULL);
SELECT * FROM dummy;
SELECT * FROM dummy WHERE b>0;
SELECT * FROM dummy WHERE NOT b<=0;
SELECT * FROM dummy WHERE NOT b>0;
SELECT * FROM dummy WHERE b<=0;


--------------------------
-- on 2), logic comparison
--------------------------

-- let's test this systematically
DROP table dummy;
CREATE TABLE dummy (a BOOLEAN, b BOOLEAN);
INSERT INTO dummy VALUES (TRUE, TRUE);
INSERT INTO dummy VALUES (TRUE, FALSE);
INSERT INTO dummy VALUES (TRUE, NULL);
INSERT INTO dummy VALUES (FALSE, TRUE);
INSERT INTO dummy VALUES (FALSE, FALSE);
INSERT INTO dummy VALUES (FALSE, NULL);
INSERT INTO dummy VALUES (NULL, TRUE);
INSERT INTO dummy VALUES (NULL, FALSE);
INSERT INTO dummy VALUES (NULL, NULL);


-- GOTCHA: NULL OR TRUE gives NULL which is wrong (TRUE known to give TRUE whatever the other  expression is)
-- GOTCHA: NULL AND FALSE gives NULL which is wrong (FALSE known to give FALSE whatever the other expression is)
SELECT 
  a
, b
, NOT a   AS "NOT_a"
, a=b     AS "a_EQ_b"
, a<>b    AS "a_NE_b"
, a<b     AS "a_LT_b"
, a<=b    AS "a_LE_b"
, a>b     AS "a_GT_b"
, a>=b    AS "a_GE_b"
, a OR b  AS "a_OR_b"
, a AND b AS "a_AND_b"
FROM dummy
;

DROP table dummy;
