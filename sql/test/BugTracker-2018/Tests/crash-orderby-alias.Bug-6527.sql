
create table "t1" ("d1" int);
insert into "t1" values (1), (2), (3), (4), (5);

-- Works
select
    "d1" as "value"
from "t1"
order by "value";


-- Works
select
    a."value"
from (
    select
        "d1" as "value"
    from "t1"
    order by "d1"
) as a;


-- Crashes
select
    a."value"
from (
    select
        "d1" as "value"
    from "t1"
    order by "value"
) as a;

drop table t1;
