create table analytics (aa int, bb int, cc bigint);

start transaction;
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);

select cast(sum(aa) over (rows between 5 preceding and 0 following) as bigint) from analytics;
select cast(sum(aa) over (rows between 5 preceding and 2 following) as bigint) from analytics;
select cast(sum(aa) over (partition by bb order by bb rows between 5 preceding and 0 following) as bigint) from analytics;
select cast(sum(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) as bigint) from analytics;

select cast(prod(aa) over (rows between 5 preceding and 0 following) as bigint) from analytics;
select cast(prod(aa) over (rows between 5 preceding and 2 following) as bigint) from analytics;
select cast(prod(aa) over (partition by bb order by bb rows between 5 preceding and 0 following) as bigint) from analytics;
select cast(prod(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) as bigint) from analytics;

select avg(aa) over (rows between 5 preceding and 0 following) from analytics;
select avg(aa) over (rows between 5 preceding and 2 following) from analytics;
select avg(aa) over (partition by bb order by bb rows between 5 preceding and 0 following) from analytics;
select avg(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) from analytics;

select min(aa) over (rows between 5 preceding and 0 following) from analytics;
select min(aa) over (rows between 5 preceding and 2 following) from analytics;
select min(aa) over (partition by bb order by bb rows between 5 preceding and 0 following) from analytics;
select min(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) from analytics;

select max(aa) over (rows between 5 preceding and 0 following) from analytics;
select max(aa) over (rows between 5 preceding and 2 following) from analytics;
select max(aa) over (partition by bb order by bb rows between 5 preceding and 0 following) from analytics;
select max(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) from analytics;

select min(bb) over (rows between 5 preceding and 0 following) from analytics;
select min(bb) over (rows between 5 preceding and 2 following) from analytics;
select min(bb) over (partition by aa order by aa rows between 5 preceding and 0 following) from analytics;
select min(bb) over (partition by aa order by aa rows between 5 preceding and 2 following) from analytics;

select max(bb) over (rows between 5 preceding and 0 following) from analytics;
select max(bb) over (rows between 5 preceding and 2 following) from analytics;
select max(bb) over (partition by aa order by aa rows between 5 preceding and 0 following) from analytics;
select max(bb) over (partition by aa order by aa rows between 5 preceding and 2 following) from analytics;

select count(*) over (rows between 5 preceding and 0 following) from analytics;
select count(*) over (rows between 5 preceding and 2 following) from analytics;
select count(*) over (partition by bb order by bb rows between 5 preceding and 0 following) from analytics;
select count(*) over (partition by bb order by bb rows between 5 preceding and 2 following) from analytics;

select count(aa) over (rows between 5 preceding and 0 following) from analytics;
select count(aa) over (rows between 5 preceding and 2 following) from analytics;
select count(aa) over (partition by bb order by bb rows between 5 preceding and 0 following) from analytics;
select count(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) from analytics;

select count(bb) over (rows between 5 preceding and 0 following) from analytics;
select count(bb) over (rows between 5 preceding and 2 following) from analytics;
select count(bb) over (partition by aa order by aa rows between 5 preceding and 0 following) from analytics;
select count(bb) over (partition by aa order by aa rows between 5 preceding and 2 following) from analytics;

select count(*) over (rows between unbounded preceding and unbounded following) from analytics;
select count(*) over (partition by bb order by bb rows between unbounded preceding and unbounded following) from analytics;
select count(aa) over (rows between unbounded preceding and unbounded following) from analytics;
select count(aa) over (partition by bb order by bb rows between unbounded preceding and unbounded following) from analytics;
select count(bb) over (rows between unbounded preceding and unbounded following) from analytics;
select count(bb) over (partition by aa order by aa rows between unbounded preceding and unbounded following) from analytics;

select count(*) over (rows unbounded preceding);
select count(*) over (rows 200 preceding);
select count(*) over (rows between 5 preceding and 0 following);

select min(aa) over (rows unbounded preceding) from analytics;
select min(aa) over (partition by bb order by bb rows unbounded preceding) from analytics;
select max(aa) over (rows unbounded preceding) from analytics;
select max(aa) over (partition by bb order by bb rows unbounded preceding) from analytics;

select cast(sum(aa) over (rows unbounded preceding) as bigint) from analytics;
select cast(sum(aa) over (partition by bb order by bb rows unbounded preceding) as bigint) from analytics;
select cast(prod(aa) over (rows unbounded preceding) as bigint) from analytics;
select cast(prod(aa) over (partition by bb order by bb rows unbounded preceding) as bigint) from analytics;

select avg(aa) over (rows unbounded preceding) from analytics;
select avg(aa) over (partition by bb order by bb rows unbounded preceding) from analytics;

select count(*) over (rows unbounded preceding) from analytics;
select count(*) over (partition by bb order by bb rows unbounded preceding) from analytics;
select count(aa) over (rows unbounded preceding) from analytics;
select count(aa) over (partition by bb order by bb rows unbounded preceding) from analytics;

select min(aa) over (rows 2 preceding) from analytics;
select min(aa) over (partition by bb order by bb rows 2 preceding) from analytics;
select max(aa) over (rows 2 preceding) from analytics;
select max(aa) over (partition by bb order by bb rows 2 preceding) from analytics;

select cast(sum(aa) over (rows 2 preceding) as bigint) from analytics;
select cast(sum(aa) over (partition by bb order by bb rows 2 preceding) as bigint) from analytics;
select cast(prod(aa) over (rows 2 preceding) as bigint) from analytics;
select cast(prod(aa) over (partition by bb order by bb rows 2 preceding) as bigint) from analytics;

select avg(aa) over (rows 2 preceding) from analytics;
select avg(aa) over (partition by bb order by bb rows 2 preceding) from analytics;

select count(*) over (rows 2 preceding) from analytics;
select count(*) over (partition by bb order by bb rows 2 preceding) from analytics;
select count(aa) over (rows 2 preceding) from analytics;
select count(aa) over (partition by bb order by bb rows 2 preceding) from analytics;

create table stressme (aa varchar(64), bb int);
insert into stressme values ('one', 1), ('another', 1), ('stress', 1), (NULL, 2), ('ok', 2), ('check', 3), ('me', 3), ('please', 3), (NULL, 4);

select avg(bb) over (rows between 5 preceding and 0 following) from stressme;
select avg(bb) over (rows between 5 preceding and 2 following) from stressme;
select avg(bb) over (partition by bb order by bb rows between 5 preceding and 0 following) from stressme;
select avg(bb) over (partition by bb order by bb rows between 5 preceding and 2 following) from stressme;

select min(aa) over (rows between 5 preceding and 0 following) from stressme;
select min(aa) over (rows between 5 preceding and 2 following) from stressme;
select min(aa) over (partition by bb order by bb rows between 5 preceding and 0 following) from stressme;
select min(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) from stressme;

select max(aa) over (rows between 5 preceding and 0 following) from stressme;
select max(aa) over (rows between 5 preceding and 2 following) from stressme;
select max(aa) over (partition by bb order by bb rows between 5 preceding and 0 following) from stressme;
select max(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) from stressme;

select count(aa) over (rows between 5 preceding and 0 following) from stressme;
select count(aa) over (rows between 5 preceding and 2 following) from stressme;
select count(aa) over (partition by bb order by bb rows between 5 preceding and 0 following) from stressme;
select count(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) from stressme;

select count(aa) over (rows between unbounded preceding and unbounded following) from stressme;
select count(aa) over (partition by bb order by bb rows between unbounded preceding and unbounded following) from stressme;

create table debugme (aa real, bb int);
insert into debugme values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select sum(aa) over (rows between 2 preceding and 0 following) from debugme;
select sum(aa) over (rows between 2 preceding and 2 following) from debugme;
select sum(aa) over (partition by bb order by bb rows between 2 preceding and 0 following) from debugme;
select sum(aa) over (partition by bb order by bb rows between 2 preceding and 2 following) from debugme;

select prod(aa) over (rows between 2 preceding and 0 following) from debugme;
select prod(aa) over (rows between 2 preceding and 2 following) from debugme;
select prod(aa) over (partition by bb order by bb rows between 2 preceding and 0 following) from debugme;
select prod(aa) over (partition by bb order by bb rows between 2 preceding and 2 following) from debugme;

select avg(aa) over (rows between 2 preceding and 0 following) from debugme;
select avg(aa) over (rows between 2 preceding and 2 following) from debugme;
select avg(aa) over (partition by bb order by bb rows between 2 preceding and 0 following) from debugme;
select avg(aa) over (partition by bb order by bb rows between 2 preceding and 2 following) from debugme;

create table overflowme (a int);
insert into overflowme values (2147483644), (2147483645), (2147483646);
select floor(avg(a) over (rows between 2 preceding and 0 following)) from overflowme;
select floor(avg(a) over (rows between 2 preceding and 2 following)) from overflowme;

rollback;

select rank() over (rows unbounded preceding) from analytics; --error
select dense_rank() over (rows 200 preceding) from analytics; --error
select ntile(1) over (rows 200 preceding) from analytics; --error
select lead(aa) over (partition by bb order by bb rows between 2 preceding and 0 following) from analytics; --error

drop table analytics;
