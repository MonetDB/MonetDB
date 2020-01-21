select 1 as T, 2 as T;
select a.* from (select 1 as T, 2 as T) a;
with wa as (select 1 as T, 2 as T) select wa.* from wa;

select 1,2,3 as "L2";
select a.* from (select 1,2,3 as "L2") a;
with wa as (select 1,2,3 as "L2") select wa.* from wa;

