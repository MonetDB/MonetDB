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

select count(*) from "groupings" group by "aa" > 1;
select "aa" > 1 from "groupings" group by "aa" > 1;
select count(*) from "groupings" group by case when "aa" > 1 then "aa" else "aa" + 10 end;
select case when "aa" > 1 then "aa" else "aa" * 4 end from "groupings" group by case when "aa" > 1 then "aa" else "aa" * 4 end;

select cast(sum("aa"+"bb") as bigint) from "groupings" group by "aa"+"bb";
select cast(sum("aa"+3452) as bigint) from "groupings" group by "aa"+"bb";
rollback;

select "aa"+3452 from "groupings" group by "aa"+"bb"; --error
select count(*) from "groupings" group by rank() over (); --error
select count(*) from "groupings" having rank() over (); --error
select count(*) from "groupings" order by rank() over (); --error TODO?
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1 having "bb"+1 > 2; --error
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1 order by "bb"+1; --error

drop table "groupings";
