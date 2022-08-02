-- http://bugs.monetdb.org/2732
with t(p,s) as
 (select pre, size from (VALUES (1,4),(2,1),(3,0),(4,1),(5,0)) as t(pre,size))

select t1.p as p1, t2.p as p2, t3.p as p3
  from t as t1, t as t2, t as t3
 where t1.p = 1
   and t2.p between (t1.p + 1) and (t1.p + t1.s)
   and t3.p between (t2.p + 1) and (t2.p + t2.s);


-- non-between version
with t(p,s) as
 (select pre, size from (VALUES (1,4),(2,1),(3,0),(4,1),(5,0)) as t(pre,size))

select t1.p as p1, t2.p as p2, t3.p as p3
  from t as t1, t as t2, t as t3
 where t1.p = 1
   and t2.p >= (t1.p + 1) and t2.p <= (t1.p + t1.s)
   and t3.p >= (t2.p + 1) and t3.p <= (t2.p + t2.s);

