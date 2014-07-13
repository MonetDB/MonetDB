
CREATE TABLE t4311 ( "a"    INT, "b"    INT);

-- The following 3 queries are EQUIVALENT

plan 
SELECT t2.a
FROM  t4311 t1, t4311 t2
WHERE t2.a between t1.a and t1.b;

plan 
SELECT t2.a
FROM  t4311 t1, t4311 t2
WHERE t2.a >= t1.a 
AND   t2.a <= t1.b;

plan 
SELECT t2.a
FROM  t4311 t1, t4311 t2
WHERE t1.a <= t2.a 
AND   t1.b >= t2.a;

drop table t4311;
