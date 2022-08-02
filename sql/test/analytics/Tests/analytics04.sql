create table analytics (aa int, bb int, cc bigint);

start transaction;
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);

select count(*) over (rows between 3 preceding and 2 preceding),
       count(*) over (rows between 2 following and 3 following),
       count(*) over (rows between 3 preceding and 3 preceding),
       count(*) over (rows between 3 preceding and 9 preceding),
       count(*) over (rows between current row and current row),
       count(*) over (rows between 0 following and 0 following),
       count(*) over (rows between 0 preceding and 0 preceding) from analytics;

select count(aa) over (partition by bb order by bb rows between 3 preceding and 2 preceding),
       count(aa) over (partition by bb order by bb rows between 2 following and 3 following),
       count(aa) over (partition by bb order by bb rows between 3 preceding and 3 preceding),
       count(aa) over (partition by bb order by bb rows between 3 preceding and 9 preceding),
       count(aa) over (partition by bb order by bb rows between current row and current row),
       count(aa) over (partition by bb order by bb rows between 0 following and 0 following),
       count(aa) over (partition by bb order by bb rows between 0 preceding and 0 preceding) from analytics;

select cast(sum(aa) over (rows between 3 preceding and 2 preceding) as bigint),
       cast(sum(aa) over (rows between 2 following and 3 following) as bigint),
       cast(sum(aa) over (rows between 3 preceding and 3 preceding) as bigint),
       cast(sum(aa) over (rows between 3 preceding and 9 preceding) as bigint),
       cast(sum(aa) over (rows between current row and current row) as bigint),
       cast(sum(aa) over (rows between 0 following and 0 following) as bigint),
       cast(sum(aa) over (rows between 0 preceding and 0 preceding) as bigint) from analytics;

select cast(sum(aa) over (partition by bb order by bb rows between 3 preceding and 2 preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb rows between 2 following and 3 following) as bigint),
       cast(sum(aa) over (partition by bb order by bb rows between 3 preceding and 3 preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb rows between 3 preceding and 9 preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb rows between current row and current row) as bigint),
       cast(sum(aa) over (partition by bb order by bb rows between 0 following and 0 following) as bigint),
       cast(sum(aa) over (partition by bb order by bb rows between 0 preceding and 0 preceding) as bigint) from analytics;

rollback;

select count(*) over (order by bb range between 3 preceding and 2 preceding),
       count(*) over (order by bb range between 2 following and 3 following),
       count(*) over (order by bb range between 3 preceding and 3 preceding),
       count(*) over (order by bb range between 3 preceding and 9 preceding),
       count(*) over (order by bb range between current row and current row),
       count(*) over (order by bb range between 0 following and 0 following),
       count(*) over (order by bb range between 0 preceding and 0 preceding) from analytics; --TODO someone brave to do add support to this...

select cast(sum(aa) over (partition by bb order by bb range between 3 preceding and 2 preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb range between 2 following and 3 following) as bigint),
       cast(sum(aa) over (partition by bb order by bb range between 3 preceding and 3 preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb range between 3 preceding and 9 preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb range between current row and current row) as bigint),
       cast(sum(aa) over (partition by bb order by bb range between 0 following and 0 following) as bigint),
       cast(sum(aa) over (partition by bb order by bb range between 0 preceding and 0 preceding) as bigint) from analytics; --TODO someone brave to do add support to this...

select count(*) over (order by bb groups between 3 preceding and 2 preceding),
       count(*) over (order by bb groups between 2 following and 3 following),
       count(*) over (order by bb groups between 3 preceding and 3 preceding),
       count(*) over (order by bb groups between 3 preceding and 9 preceding),
       count(*) over (order by bb groups between current row and current row),
       count(*) over (order by bb groups between 0 following and 0 following),
       count(*) over (order by bb groups between 0 preceding and 0 preceding) from analytics; --TODO someone brave to do add support to this...

select cast(sum(aa) over (partition by bb order by bb groups between 3 preceding and 2 preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb groups between 2 following and 3 following) as bigint),
       cast(sum(aa) over (partition by bb order by bb groups between 3 preceding and 3 preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb groups between 3 preceding and 9 preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb groups between current row and current row) as bigint),
       cast(sum(aa) over (partition by bb order by bb groups between 0 following and 0 following) as bigint),
       cast(sum(aa) over (partition by bb order by bb groups between 0 preceding and 0 preceding) as bigint) from analytics; --TODO someone brave to do add support to this...

select count(*) over (rows between 3 following and 2 preceding) from analytics; --error
select count(*) over (rows between current row and 2 preceding) from analytics; --error
select count(*) over (rows between 3 following and current row) from analytics; --error
select count(distinct aa) over (rows between 1 preceding and current row) from analytics; --error, distinct not implemented

drop table analytics;
