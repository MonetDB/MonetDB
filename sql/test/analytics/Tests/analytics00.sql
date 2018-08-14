start transaction;
create table analytics (aa int, bb int, cc bigint);
insert into analytics values (3, 1, 3), (2, 1, 2), (4, 1, 4), (8, 2, 8), (3, 2, 3), (5, 3, 5), (6, 3, 6), (15, 3, 15);

select min(aa) over (partition by bb) from analytics;
select min(aa) over (partition by bb order by bb asc) from analytics;
select min(aa) over (partition by bb order by bb desc) from analytics;
select min(aa) over (order by bb desc) from analytics;

select max(aa) over (partition by bb) from analytics;
select max(aa) over (partition by bb order by bb asc) from analytics;
select max(aa) over (partition by bb order by bb desc) from analytics;
select max(aa) over (order by bb desc) from analytics;

select min(cc) over (partition by bb) from analytics;
select min(cc) over (partition by bb order by bb asc) from analytics;
select min(cc) over (partition by bb order by bb desc) from analytics;
select min(cc) over (order by bb desc) from analytics;

select max(cc) over (partition by bb) from analytics;
select max(cc) over (partition by bb order by bb asc) from analytics;
select max(cc) over (partition by bb order by bb desc) from analytics;
select max(cc) over (order by bb desc) from analytics;
rollback;
