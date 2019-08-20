-- no cast to bigint (from hugeint) since the use of hugeint was part of the bug

select foo,
       sum(foo) over () as s1,
       1.0 * sum(foo) over () as s2,
       1.0 * cast(sum(foo) over () as float) as s3
  from (select 7 as foo union all select 3 as foo) as t;

select foo,
       sum(foo) over () / 2
  from (select 7 as foo union all select 3 as foo) as t;

select foo,
       sum(foo) over (order by foo) as sumsum1,
       1.0 * sum(foo) over (order by foo) as cumsum2
  from (values (1),(2),(3),(4),(5),(6)) as t(foo);
