--
-- AGGREGATES
--

SELECT avg(four) AS avg_1 FROM onek;

SELECT avg(a) AS avg_32 FROM aggtest WHERE a < 100;

-- In 7.1, avg(float4) is computed using float8 arithmetic.
-- Round the result to 3 digits to avoid platform-specific results.

SELECT avg(b)::numeric(10,3) AS avg_107_943 FROM aggtest;

SELECT avg(gpa) AS avg_3_4 FROM ONLY student;


SELECT sum(four) AS sum_1500 FROM onek;

SELECT sum(a) AS sum_198 FROM aggtest;

SELECT sum(b) AS avg_431_773 FROM aggtest;

SELECT sum(gpa) AS avg_6_8 FROM ONLY student;


SELECT max(four) AS max_3 FROM onek;

SELECT max(a) AS max_100 FROM aggtest;

SELECT max(aggtest.b) AS max_324_78 FROM aggtest;

SELECT max(student.gpa) AS max_3_7 FROM student;


SELECT count(four) AS cnt_1000 FROM onek;

SELECT count(DISTINCT four) AS cnt_4 FROM onek;

select ten, count(*), sum(four) from onek
group by ten order by ten;

select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;


SELECT newavg(four) AS avg_1 FROM onek;

SELECT newsum(four) AS sum_1500 FROM onek;

SELECT newcnt(four) AS cnt_1000 FROM onek;


-- test for outer-level aggregates

-- this should work
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- this should fail because subquery has an agg of its own in WHERE
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b
               where sum(distinct a.four + b.four) = b.four);

--
-- test for bitwise integer aggregates
--
CREATE TEMPORARY TABLE bitwise_test(
  i2 INT2,
  i4 INT4,
  i8 INT8,
  i INTEGER,
  x INT2,
  y BIT(4)
);

-- empty case
SELECT 
  BIT_AND(i2) AS "?",
  BIT_OR(i4)  AS "?"
FROM bitwise_test;

COPY bitwise_test FROM STDIN NULL 'null';
1	1	1	1	1	B0101
3	3	3	null	2	B0100
7	7	7	3	4	B1100
\.

SELECT
  BIT_AND(i2) AS "1",
  BIT_AND(i4) AS "1",
  BIT_AND(i8) AS "1",
  BIT_AND(i)  AS "?",
  BIT_AND(x)  AS "0",
  BIT_AND(y)  AS "0100",

  BIT_OR(i2)  AS "7",
  BIT_OR(i4)  AS "7",
  BIT_OR(i8)  AS "7",
  BIT_OR(i)   AS "?",
  BIT_OR(x)   AS "7",
  BIT_OR(y)   AS "1101"
FROM bitwise_test;

--
-- test boolean aggregates
--
-- first test all possible transition and final states

SELECT
  -- boolean and transitions
  -- null because strict
  booland_statefunc(NULL, NULL)  IS NULL AS "t",
  booland_statefunc(TRUE, NULL)  IS NULL AS "t",
  booland_statefunc(FALSE, NULL) IS NULL AS "t",
  booland_statefunc(NULL, TRUE)  IS NULL AS "t",
  booland_statefunc(NULL, FALSE) IS NULL AS "t",
  -- and actual computations
  booland_statefunc(TRUE, TRUE) AS "t",
  NOT booland_statefunc(TRUE, FALSE) AS "t",
  NOT booland_statefunc(FALSE, TRUE) AS "t",
  NOT booland_statefunc(FALSE, FALSE) AS "t";

SELECT
  -- boolean or transitions
  -- null because strict
  boolor_statefunc(NULL, NULL)  IS NULL AS "t",
  boolor_statefunc(TRUE, NULL)  IS NULL AS "t",
  boolor_statefunc(FALSE, NULL) IS NULL AS "t",
  boolor_statefunc(NULL, TRUE)  IS NULL AS "t",
  boolor_statefunc(NULL, FALSE) IS NULL AS "t",
  -- actual computations
  boolor_statefunc(TRUE, TRUE) AS "t",
  boolor_statefunc(TRUE, FALSE) AS "t",
  boolor_statefunc(FALSE, TRUE) AS "t",
  NOT boolor_statefunc(FALSE, FALSE) AS "t";

CREATE TEMPORARY TABLE bool_test(  
  b1 BOOL,
  b2 BOOL,
  b3 BOOL,
  b4 BOOL);

-- empty case
SELECT
  BOOL_AND(b1)   AS "n",
  BOOL_OR(b3)    AS "n"
FROM bool_test;

COPY bool_test FROM STDIN NULL 'null';
TRUE	null	FALSE	null
FALSE	TRUE	null	null
null	TRUE	FALSE	null
\.

SELECT
  BOOL_AND(b1)     AS "f",
  BOOL_AND(b2)     AS "t",
  BOOL_AND(b3)     AS "f",
  BOOL_AND(b4)     AS "n",
  BOOL_AND(NOT b2) AS "f",
  BOOL_AND(NOT b3) AS "t"
FROM bool_test;

SELECT
  EVERY(b1)     AS "f",
  EVERY(b2)     AS "t",
  EVERY(b3)     AS "f",
  EVERY(b4)     AS "n",
  EVERY(NOT b2) AS "f",
  EVERY(NOT b3) AS "t"
FROM bool_test;

SELECT
  BOOL_OR(b1)      AS "t",
  BOOL_OR(b2)      AS "t",
  BOOL_OR(b3)      AS "f",
  BOOL_OR(b4)      AS "n",
  BOOL_OR(NOT b2)  AS "f",
  BOOL_OR(NOT b3)  AS "t"
FROM bool_test;
