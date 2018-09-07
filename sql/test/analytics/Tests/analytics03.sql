start transaction;

create table rowsvsrange (aa int, bb int, cc real);
insert into rowsvsrange values (1,1,1), (2,1,2), (3,1,3), (1,2,1), (1,2,1), (1,2,1), (2,2,2), (3,2,3), (4,2,4), (2,2,2);

select cast(sum(aa) over (rows unbounded preceding) as bigint),
       cast(sum(aa) over (range unbounded preceding) as bigint),
       cast(sum(aa) over (order by aa rows unbounded preceding) as bigint),
       cast(sum(aa) over (order by aa range unbounded preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb rows unbounded preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb range unbounded preceding) as bigint) from rowsvsrange;

select sum(cc) over (rows unbounded preceding),
       sum(cc) over (range unbounded preceding),
       sum(cc) over (order by cc rows unbounded preceding),
       sum(cc) over (order by cc range unbounded preceding),
       sum(cc) over (partition by bb order by bb rows unbounded preceding),
       sum(cc) over (partition by bb order by bb range unbounded preceding) from rowsvsrange;

select cast(sum(aa) over (order by aa range between current row and unbounded following) as bigint) from rowsvsrange;
select sum(cc) over (order by cc range between current row and unbounded following) from rowsvsrange;
select count(*) over (order by cc range between current row and unbounded following) from rowsvsrange;
select count(aa) over (order by cc range between current row and unbounded following) from rowsvsrange;
select min(aa) over (order by cc range between current row and unbounded following) from rowsvsrange;
select max(aa) over (order by cc range between current row and unbounded following) from rowsvsrange;
select avg(aa) over (order by cc range between current row and unbounded following) from rowsvsrange;
select avg(cc) over (order by aa range between current row and unbounded following) from rowsvsrange;

create table analytics (aa int, bb int, cc bigint);
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);

select count(*) over (rows between current row and unbounded following),
       count(*) over (range between current row and unbounded following),
       count(*) over (order by bb rows between current row and unbounded following),
       count(*) over (order by bb range between current row and unbounded following),
       count(*) over (partition by bb order by bb rows unbounded preceding),
       count(*) over (partition by bb order by bb range unbounded preceding) from analytics;

select count(aa) over (rows between current row and unbounded following),
       count(aa) over (range between current row and unbounded following),
       count(aa) over (order by bb rows between current row and unbounded following),
       count(aa) over (order by bb range between current row and unbounded following),
       count(aa) over (partition by bb order by bb rows unbounded preceding),
       count(aa) over (partition by bb order by bb range unbounded preceding) from analytics;

select min(aa) over (rows between current row and unbounded following),
       min(aa) over (range between current row and unbounded following),
       min(aa) over (order by bb rows between current row and unbounded following),
       min(aa) over (order by bb range between current row and unbounded following),
       min(aa) over (partition by bb order by bb rows unbounded preceding),
       min(aa) over (partition by bb order by bb range unbounded preceding) from analytics;

select max(aa) over (rows between current row and unbounded following),
       max(aa) over (range between current row and unbounded following),
       max(aa) over (order by bb rows between current row and unbounded following),
       max(aa) over (order by bb range between current row and unbounded following),
       max(aa) over (partition by bb order by bb rows unbounded preceding),
       max(aa) over (partition by bb order by bb range unbounded preceding) from analytics;

select avg(aa) over (rows between current row and unbounded following),
       avg(aa) over (range between current row and unbounded following),
       avg(aa) over (order by bb rows between current row and unbounded following),
       avg(aa) over (order by bb range between current row and unbounded following),
       avg(aa) over (partition by bb order by bb rows unbounded preceding),
       avg(aa) over (partition by bb order by bb range unbounded preceding) from analytics;

create table stressme (aa varchar(64), bb int);
insert into stressme values ('one', 1), ('another', 1), ('stress', 1), (NULL, 2), ('ok', 2), ('check', 3), ('me', 3), ('please', 3), (NULL, 4);

select count(aa) over (rows between current row and unbounded following),
       count(aa) over (range between current row and unbounded following),
       count(aa) over (order by bb rows between current row and unbounded following),
       count(aa) over (order by bb range between current row and unbounded following),
       count(aa) over (partition by bb order by bb rows unbounded preceding),
       count(aa) over (partition by bb order by bb range unbounded preceding) from stressme;

select min(aa) over (rows between current row and unbounded following),
       min(aa) over (range between current row and unbounded following),
       min(aa) over (order by bb rows between current row and unbounded following),
       min(aa) over (order by bb range between current row and unbounded following),
       min(aa) over (partition by bb order by bb rows unbounded preceding),
       min(aa) over (partition by bb order by bb range unbounded preceding) from stressme;

select max(aa) over (rows between current row and unbounded following),
       max(aa) over (range between current row and unbounded following),
       max(aa) over (order by bb rows between current row and unbounded following),
       max(aa) over (order by bb range between current row and unbounded following),
       max(aa) over (partition by bb order by bb rows unbounded preceding),
       max(aa) over (partition by bb order by bb range unbounded preceding) from stressme;

create table overflowme (aa int, bb int);
insert into overflowme values (2147483644, 1), (2147483645, 2), (2147483646, 1), (2147483644, 2), (2147483645, 1), (2147483646, 2);

select avg(aa) over (rows between current row and unbounded following),
       avg(aa) over (range between current row and unbounded following),
       avg(aa) over (order by bb rows between current row and unbounded following),
       avg(aa) over (order by bb range between current row and unbounded following),
       avg(aa) over (partition by bb order by bb rows unbounded preceding),
       avg(aa) over (partition by bb order by bb range unbounded preceding) from overflowme;

rollback;
