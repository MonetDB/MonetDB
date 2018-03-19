start transaction;

create table "t1" (
    "id" int,
    "c1" varchar(100),
    "d1" int,
    "d2" int
);

insert into "t1" values
(1, 'A', 50, 80),
(2, 'A', 200, 350),
(3, 'A', 89, 125),
(4, 'B', 4845, 13),
(5, 'B', 194, 597),
(6, 'C', 5636, 5802),
(7, 'C', 375, 3405),
(7, 'D', 365, 0),
(7, 'D', 87, 0);

-- Works
select
    "c1",
    sum("d1") as "d1",
    sum("d2") as "d2"
from "t1"
group by "c1"
having sum("d1") < case when 5 > 10 then 500 else 400 end;

-- Works
select
    "c1",
    sum("d1")as "d1",
    sum("d2") as "d2",
    1.0 * sum("d1") / (1.0 * case when sum("d2") > 0 then sum("d2") else null end) as "formula"
from "t1"
group by "c1";

-- Crashes
select
    "c1",
    sum("d1")as "d1",
    sum("d2") as "d2",
    1.0 * sum("d1") / (1.0 * case when sum("d2") > 0 then sum("d2") else null end) as "formula"
from "t1"
group by "c1"
having (1.0 * sum("d1") / (1.0 * case when sum("d2") > 0 then sum("d2") else null end)) > 1;

rollback;
