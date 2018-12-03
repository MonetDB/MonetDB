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

select cast(prod(aa) over (partition by bb) as bigint) from analytics;
select cast(prod(aa) over (partition by bb order by bb asc) as bigint) from analytics;
select cast(prod(aa) over (partition by bb order by bb desc) as bigint) from analytics;
select cast(prod(aa) over (order by bb desc) as bigint) from analytics;

select avg(aa) over (partition by bb) from analytics;
select avg(aa) over (partition by bb order by bb asc) from analytics;
select avg(aa) over (partition by bb order by bb desc) from analytics;
select avg(aa) over (order by bb desc) from analytics;

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

select cast(prod(cc) over (partition by bb) as bigint) from analytics;
select cast(prod(cc) over (partition by bb order by bb asc) as bigint) from analytics;
select cast(prod(cc) over (partition by bb order by bb desc) as bigint) from analytics;

select avg(cc) over (partition by bb) from analytics;
select avg(cc) over (partition by bb order by bb asc) from analytics;
select avg(cc) over (partition by bb order by bb desc) from analytics;
select avg(cc) over (order by bb desc) from analytics;

select count(cc) over (partition by bb) from analytics;
select count(cc) over (partition by bb order by bb asc) from analytics;
select count(cc) over (partition by bb order by bb desc) from analytics;
select count(cc) over (order by bb desc) from analytics;

select count(*) over (partition by bb) from analytics;
select count(*) over (partition by bb order by bb asc) from analytics;
select count(*) over (partition by bb order by bb desc) from analytics;
select count(*) over (order by bb desc) from analytics;

select min(aa) over () from analytics;
select max(aa) over () from analytics;
select cast(sum(aa) over () as bigint) from analytics;
select cast(prod(aa) over () as bigint) from analytics;
select avg(aa) over () from analytics;
select count(aa) over () from analytics;
select count(*) over () from analytics;
select count(*) over ();

select min(null) over () from analytics;
select max(null) over () from analytics;
select cast(sum(null) over () as bigint) from analytics;
select cast(prod(null) over () as bigint) from analytics;
select avg(null) over () from analytics;
select count(null) over () from analytics;

select min(2) over () from analytics;
select max(100) over () from analytics;
select cast(sum(2) over () as bigint) from analytics;
select cast(prod(4 + 0) over () as bigint) from analytics;
select avg(4) over () from analytics;
select count(4) over () from analytics;

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

select count(aa) over (partition by bb) from stressme;
select count(aa) over (partition by bb order by bb asc) from stressme;
select count(aa) over (partition by bb order by bb desc) from stressme;
select count(aa) over (order by bb desc) from stressme;

select count(*) over (partition by bb) from stressme;
select count(*) over (partition by bb order by bb asc) from stressme;
select count(*) over (partition by bb order by bb desc) from stressme;
select count(*) over (order by bb desc) from stressme;

select min(bb) over (partition by aa) from stressme;
select min(bb) over (partition by aa order by aa asc) from stressme;
select min(bb) over (partition by aa order by aa desc) from stressme;
select min(bb) over (order by aa desc) from stressme;

select max(bb) over (partition by aa) from stressme;
select max(bb) over (partition by aa order by aa asc) from stressme;
select max(bb) over (partition by aa order by aa desc) from stressme;
select max(bb) over (order by aa desc) from stressme;

select cast(sum(bb) over (partition by aa) as bigint) from stressme;
select cast(sum(bb) over (partition by aa order by aa asc) as bigint) from stressme;
select cast(sum(bb) over (partition by aa order by aa desc) as bigint) from stressme;
select cast(sum(bb) over (order by aa desc) as bigint) from stressme;

select cast(prod(bb) over (partition by aa) as bigint) from stressme;
select cast(prod(bb) over (partition by aa order by aa asc) as bigint) from stressme;
select cast(prod(bb) over (partition by aa order by aa desc) as bigint) from stressme;
select cast(prod(bb) over (order by aa desc) as bigint) from stressme;

select avg(bb) over (partition by aa) from stressme;
select avg(bb) over (partition by aa order by aa asc) from stressme;
select avg(bb) over (partition by aa order by aa desc) from stressme;
select avg(bb) over (order by aa desc) from stressme;

select count(bb) over (partition by aa) from stressme;
select count(bb) over (partition by aa order by aa asc) from stressme;
select count(bb) over (partition by aa order by aa desc) from stressme;
select count(bb) over (order by aa desc) from stressme;

select count(*) over (partition by aa) from stressme;
select count(*) over (partition by aa order by aa asc) from stressme;
select count(*) over (partition by aa order by aa desc) from stressme;
select count(*) over (order by aa desc) from stressme;

select min(2) over (partition by aa) from stressme;
select max(100) over (partition by bb) from stressme;
select cast(sum(8 / (- 2)) over (partition by aa order by aa asc) as bigint) from stressme;
select cast(prod(4 + 0) over (partition by bb order by bb asc) as bigint) from stressme;
select avg(4 + null) over (partition by bb order by bb asc) from stressme;
select count(case when 4 = 4 then 4 else 0 end) over (partition by aa order by aa asc) from stressme;

create table debugme (aa real, bb int);
insert into debugme values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select sum(aa) over (partition by bb) from debugme;
select sum(aa) over (partition by bb order by bb asc) from debugme;
select sum(aa) over (partition by bb order by bb desc) from debugme;
select sum(aa) over (order by bb desc) from debugme;

select prod(aa) over (partition by bb) from debugme;
select prod(aa) over (partition by bb order by bb asc) from debugme;
select prod(aa) over (partition by bb order by bb desc) from debugme;
select prod(aa) over (order by bb desc) from debugme;

select avg(aa) over (partition by bb) from debugme;
select avg(aa) over (partition by bb order by bb asc) from debugme;
select avg(aa) over (partition by bb order by bb desc) from debugme;
select avg(aa) over (order by bb desc) from debugme;

create table overflowme (a int); --we test on 32-bit machines up so this should be safe to test an overflow
insert into overflowme values (2147483644), (2147483645), (2147483646);
select floor(avg(a) over ()) from overflowme;

rollback;
