START TRANSACTION;

create table x (a1 int, prob double);
create table y (a1 int, prob double);
create table z (a1 int, prob double);

with xy as (
SELECT x.a1 as a1, x.prob + y.prob as prob FROM x, y WHERE x.a1=y.a1
UNION ALL
SELECT x.a1 as a1, x.prob as prob FROM x WHERE x.a1 NOT IN (select a1 from y)
UNION ALL
SELECT y.a1 as a1, y.prob as prob FROM y WHERE y.a1 NOT IN (select a1 from x)
)
SELECT xy.a1 as a1, xy.prob + z.prob as prob FROM xy, z WHERE xy.a1=z.a1
UNION ALL
SELECT xy.a1 as a1, xy.prob as prob FROM xy WHERE xy.a1 NOT IN (select a1 from z)
UNION ALL
SELECT z.a1 as a1, z.prob as prob FROM z WHERE z.a1 NOT IN (select a1 from xy);

with xy as (
SELECT x.a1 as a1, x.prob + y.prob as prob FROM x, y WHERE x.a1=y.a1
UNION ALL
SELECT x.a1 as a1, x.prob as prob FROM x WHERE x.a1 NOT IN (SELECT x.a1 FROM x, y WHERE x.a1=y.a1)
UNION ALL
SELECT y.a1 as a1, y.prob as prob FROM y WHERE y.a1 NOT IN (SELECT x.a1 FROM x, y WHERE x.a1=y.a1)
)
SELECT xy.a1 as a1, xy.prob + z.prob as prob FROM xy, z WHERE xy.a1=z.a1
UNION ALL
SELECT xy.a1 as a1, xy.prob as prob FROM xy WHERE xy.a1 NOT IN (SELECT xy.a1 FROM xy, z WHERE xy.a1=z.a1)
UNION ALL
SELECT z.a1 as a1, z.prob as prob FROM z WHERE z.a1 NOT IN (SELECT xy.a1 FROM xy, z WHERE xy.a1=z.a1);

-- same queries but now with LIMIT 5;
with xy as (
SELECT x.a1 as a1, x.prob + y.prob as prob FROM x, y WHERE x.a1=y.a1
UNION ALL
SELECT x.a1 as a1, x.prob as prob FROM x WHERE x.a1 NOT IN (select a1 from y)
UNION ALL
SELECT y.a1 as a1, y.prob as prob FROM y WHERE y.a1 NOT IN (select a1 from x)
)
SELECT xy.a1 as a1, xy.prob + z.prob as prob FROM xy, z WHERE xy.a1=z.a1
UNION ALL
SELECT xy.a1 as a1, xy.prob as prob FROM xy WHERE xy.a1 NOT IN (select a1 from z)
UNION ALL
SELECT z.a1 as a1, z.prob as prob FROM z WHERE z.a1 NOT IN (select a1 from xy)
LIMIT 5;
-- causes error: could not find xy.a1. dev/sql/backends/monet5/rel_bin.c:2387: rel2bin_project: Assertion `0' failed.

with xy as (
SELECT x.a1 as a1, x.prob + y.prob as prob FROM x, y WHERE x.a1=y.a1
UNION ALL
SELECT x.a1 as a1, x.prob as prob FROM x WHERE x.a1 NOT IN (SELECT x.a1 FROM x, y WHERE x.a1=y.a1)
UNION ALL
SELECT y.a1 as a1, y.prob as prob FROM y WHERE y.a1 NOT IN (SELECT x.a1 FROM x, y WHERE x.a1=y.a1)
)
SELECT xy.a1 as a1, xy.prob + z.prob as prob FROM xy, z WHERE xy.a1=z.a1
UNION ALL
SELECT xy.a1 as a1, xy.prob as prob FROM xy WHERE xy.a1 NOT IN (SELECT xy.a1 FROM xy, z WHERE xy.a1=z.a1)
UNION ALL
SELECT z.a1 as a1, z.prob as prob FROM z WHERE z.a1 NOT IN (SELECT xy.a1 FROM xy, z WHERE xy.a1=z.a1)
LIMIT 5;
-- causes error: could not find xy.a1. dev/sql/backends/monet5/rel_bin.c:2387: rel2bin_project: Assertion `0' failed.

drop table x;
drop table y;
drop table z;

ROLLBACK;

