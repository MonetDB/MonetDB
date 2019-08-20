create table "groupings" ("aa" int, "bb" int);

start transaction;
insert into "groupings" values (1, 1), (2, 2), (1, 3);
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1;
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1, "aa"+1;
select cast(("aa"+1) + ("bb"+1) as bigint) from "groupings" group by "aa"+1, "bb"+1;
select cast(("aa"+1) + ("bb"+1) as bigint) from "groupings" group by ("aa"+1) + ("bb"+1);

select cast(sum("aa"+1) as bigint) from "groupings" group by "aa"+1;
select cast(sum(distinct "aa"+1) as bigint) from "groupings" group by "aa"+1;
select cast(sum(("aa"+1) + ("bb"+2)) as bigint) from "groupings" group by "aa"+1, "bb"+2;
select cast(sum(("aa"+1) + ("bb"+2)) as bigint) from "groupings" group by ("aa"+1) + ("bb"+2);

select cast("aa"+1 as bigint) from "groupings" group by "aa"+1 having "aa"+1 > 2;
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1 order by "aa"+1;

create function sumints("a" int, "b" int) returns int begin return "a" + "b"; end;
select count(*) from "groupings" group by sumints("aa","bb");
select sumints("aa","bb") from "groupings" group by sumints("aa","bb");
select cast(sumints("aa","bb")*sum("bb") as bigint) from "groupings" group by sumints("aa","bb");
select cast(sum("bb") as bigint) from "groupings" group by sumints("aa","bb")*sumints("aa",19);

select count("aa") from "groupings" group by "aa" > 1;
select count(distinct "aa") from "groupings" group by "aa" > 1;
select distinct count(distinct "aa") from "groupings" group by "aa" > 1;
select "aa" > 1 from "groupings" group by "aa" > 1;
select count(*) from "groupings" group by case when "aa" > 1 then "aa" else "aa" + 10 end;
select case when "aa" > 1 then "aa" else "aa" * 4 end from "groupings" group by case when "aa" > 1 then "aa" else "aa" * 4 end;

select cast(sum("aa"+"bb") as bigint) from "groupings" group by "aa"+"bb";
select cast(sum(distinct "aa"+"bb") as bigint) from "groupings" group by "aa"+"bb";
select cast(sum("aa"+3452) as bigint) from "groupings" group by "aa"+"bb";
select cast(sum(distinct "aa"+3452) as bigint) from "groupings" group by "aa"+"bb";

select count(*) from "groupings" having count("aa"-54) > 2;
select count(*) from "groupings" order by count("bb"+1);

select 1 group by 1;
select 1 group by 2;
select -90 from "groupings" group by -90;
select count(*) from "groupings" group by 'hello';
select count(*) from "groupings" group by NULL;
select 'world' from "groupings" group by -90;

rollback;

select "aa" from "groupings" group by NULL; --error
select "aa"+3452 from "groupings" group by "aa"+"bb"; --error
select count(*) from "groupings" group by count("aa"); --error
select count(*) from "groupings" group by rank() over (); --error
select count(*) from "groupings" having rank() over (); --error
select count(*) from "groupings" order by rank() over (); --error TODO?
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1 having "bb"+1 > 2; --error
select cast("aa"+1 as bigint) from "groupings" group by "aa"+1 order by "bb"+1; --error
select case when "aa" > 2 then "aa" else "aa" * 4 end from "groupings" group by case when "aa" > 1 then "aa" else "aa" * 4 end; --error
select case when "aa" < 1 then "aa" else "aa" * 4 end from "groupings" group by case when "aa" > 1 then "aa" else "aa" * 4 end; --error

drop table "groupings";
