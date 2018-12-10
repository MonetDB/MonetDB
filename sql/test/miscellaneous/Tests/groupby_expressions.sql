start transaction;
create table "groupings" ("aa" int, "bb" int);
insert into "groupings" values (1, 1), (2, 2), (1, 3);

select cast("aa"+1 as bigint) from "groupings" group by "aa"+1;
select cast(("aa"+1) + ("bb"+1) as bigint) from "groupings" group by "aa"+1, "bb"+1;
select cast(("aa"+1) + ("bb"+1) as bigint) from "groupings" group by ("aa"+1) + ("bb"+1);

select cast(sum("aa"+1) as bigint) from "groupings" group by "aa"+1;
select cast(sum(("aa"+1) + ("bb"+2)) as bigint) from "groupings" group by "aa"+1, "bb"+2;
select cast(sum(("aa"+1) + ("bb"+2)) as bigint) from "groupings" group by ("aa"+1) + ("bb"+2);
rollback;
