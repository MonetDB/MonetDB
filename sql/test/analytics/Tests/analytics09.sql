create table analytics (aa int, bb int);
insert into analytics values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select sum(1) over (), rank() over (), nth_value(1, 1) over ();

select avg(sum(aa) over ()) from analytics;

select sum(1) * count(*) over ();

select sum(aa) * count(*) over () from analytics;

select aa * count(1) over () from analytics;

select sum(aa) * count(1) over () from analytics;

select sum(aa) * count(1 + aa) / avg(1) over () from analytics;

select avg(sum(aa)) over () from analytics;

select sum(aa) * 100 / sum(sum(aa)) over (partition by bb) from analytics; --error

select prod(sum(aa)) * count(1 + aa) / avg(null) over () from analytics; --error

select rank() over (partition by case when aa > 5 then aa else aa + 5 end) from analytics; --error, we don't support expressions in partition by as well group by

select rank() over (partition by sum(aa)) from analytics; --error, we don't support expressions in partition by as well group by

drop table analytics;
