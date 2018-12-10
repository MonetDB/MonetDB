create table "groupings" ("aa" int, "bb" int);

start transaction;
insert into "groupings" values (1, 1), (2, 2), (1, 3);
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1;
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1, "aa"+1;
select cast(("aa"+1) + ("bb"+1) as bigint) from "groupings" group by "aa"+1, "bb"+1;
select cast(("aa"+1) + ("bb"+1) as bigint) from "groupings" group by ("aa"+1) + ("bb"+1);

select cast(sum("aa"+1) as bigint) from "groupings" group by "aa"+1;
select cast(sum(("aa"+1) + ("bb"+2)) as bigint) from "groupings" group by "aa"+1, "bb"+2;
select cast(sum(("aa"+1) + ("bb"+2)) as bigint) from "groupings" group by ("aa"+1) + ("bb"+2);

select cast("aa"+1 as bigint) from "groupings" group by "aa"+1 having "aa"+1 > 2;
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1 order by "aa"+1;
rollback;

select cast("aa"+1 as bigint) from "groupings" group by "aa"+1 having "bb"+1 > 2; --error
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1 order by "bb"+1; --error

drop table "groupings";
