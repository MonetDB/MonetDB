start transaction;
create table analytics (aa int, bb int, cc bigint);
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);

select min(aa) over (partition by bb) from analytics;
select min(aa) over (partition by bb order by bb asc) from analytics;
select min(aa) over (partition by bb order by bb desc) from analytics;
select min(aa) over (order by bb desc) from analytics;

select max(aa) over (partition by bb) from analytics;
select max(aa) over (partition by bb order by bb asc) from analytics;
select max(aa) over (partition by bb order by bb desc) from analytics;
select max(aa) over (order by bb desc) from analytics;

select cast(sum(aa) over (partition by bb) as bigint) from analytics;
select cast(sum(aa) over (partition by bb order by bb asc) as bigint) from analytics;
select cast(sum(aa) over (partition by bb order by bb desc) as bigint) from analytics;
select cast(sum(aa) over (order by bb desc) as bigint) from analytics;

select count(aa) over (partition by bb) from analytics;
select count(aa) over (partition by bb order by bb asc) from analytics;
select count(aa) over (partition by bb order by bb desc) from analytics;
select count(aa) over (order by bb desc) from analytics;

select min(cc) over (partition by bb) from analytics;
select min(cc) over (partition by bb order by bb asc) from analytics;
select min(cc) over (partition by bb order by bb desc) from analytics;
select min(cc) over (order by bb desc) from analytics;

select max(cc) over (partition by bb) from analytics;
select max(cc) over (partition by bb order by bb asc) from analytics;
select max(cc) over (partition by bb order by bb desc) from analytics;
select max(cc) over (order by bb desc) from analytics;

select cast(sum(cc) over (partition by bb) as bigint) from analytics;
select cast(sum(cc) over (partition by bb order by bb asc) as bigint) from analytics;
select cast(sum(cc) over (partition by bb order by bb desc) as bigint) from analytics;
select cast(sum(cc) over (order by bb desc) as bigint) from analytics;

select count(cc) over (partition by bb) from analytics;
select count(cc) over (partition by bb order by bb asc) from analytics;
select count(cc) over (partition by bb order by bb desc) from analytics;
select count(cc) over (order by bb desc) from analytics;

select count(*) over (partition by bb) from analytics;
select count(*) over (partition by bb order by bb asc) from analytics;
select count(*) over (partition by bb order by bb desc) from analytics;
select count(*) over (order by bb desc) from analytics;

create table stressme (aa varchar(64), bb int);
insert into stressme values ('one', 1), ('another', 1), ('stress', 1), (NULL, 2), ('ok', 2), ('check', 3), ('me', 3), ('please', 3), (NULL, 4);

select min(aa) over (partition by bb) from stressme;
select min(aa) over (partition by bb order by bb asc) from stressme;
select min(aa) over (partition by bb order by bb desc) from stressme;
select min(aa) over (order by bb desc) from stressme;

select max(aa) over (partition by bb) from stressme;
select max(aa) over (partition by bb order by bb asc) from stressme;
select max(aa) over (partition by bb order by bb desc) from stressme;
select max(aa) over (order by bb desc) from stressme;

create table debugme (aa real, bb int);
insert into debugme values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select sum(aa) over (partition by bb) from debugme;
select sum(aa) over (partition by bb order by bb asc) from debugme;
select sum(aa) over (partition by bb order by bb desc) from debugme;
select sum(aa) over (order by bb desc) from debugme;

rollback;
