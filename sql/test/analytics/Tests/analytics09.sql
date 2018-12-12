create table analytics (aa int, bb int);
insert into analytics values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select cast(sum(1) over () as bigint), rank() over (), nth_value(1, 1) over ();

select avg(sum(aa) over ()) from analytics;

select cast(sum(1) * count(*) over () as bigint);

select cast(sum(aa) * count(*) over () as bigint) from analytics;

select cast(aa * count(1) over () as bigint) from analytics;

select cast(sum(aa) * count(1) over () as bigint) from analytics;

select cast(sum(aa) * count(1 + aa) / avg(1) over () as bigint) from analytics;

select avg(sum(aa)) over () from analytics;

select sum(cast(aa as double)) over (rows unbounded preceding) from analytics;

select sum(cast(aa as double)) over (range unbounded preceding) from analytics;

select avg(avg(aa)) over (rows unbounded preceding) from analytics;

select avg(avg(aa)) over (range unbounded preceding) from analytics;

select avg(sum(aa)) over (rows unbounded preceding) from analytics;

select avg(sum(aa)) over (range unbounded preceding) from analytics;

select avg(sum(aa)) over (), avg(avg(aa)) over () from analytics; --TODO support multiple aggregations within the same projection

select avg(sum(aa)) over (),
       cast(sum(aa) * count(case when bb < 2 then bb - 1 else bb + 1 end) / avg(1) over (rows between current row and current row) as bigint),
       avg(sum(aa)) over (rows unbounded preceding),
       avg(sum(aa)) over (range unbounded preceding) from analytics; --TODO support multiple aggregations within the same projection

select avg(sum(aa)) over () from analytics group by aa;

select avg(sum(aa)) over (),
       avg(sum(aa)) over (rows unbounded preceding),
       cast(sum(aa) * count(aa) / avg(aa) over (rows between current row and unbounded following) as bigint),
       avg(sum(aa)) over (range unbounded preceding) from analytics group by aa; --TODO support multiple aggregations within the same projection

select avg(sum(aa)) over (partition by bb) from analytics group by bb;

select cast(sum(aa) * 100 / sum(sum(aa)) over () as bigint) from analytics;

select cast(sum(aa) * 100 / sum(sum(aa)) over (partition by bb) as bigint) from analytics group by bb;

select cast(sum(aa) * 100 / sum(sum(aa)) over (partition by bb) as bigint) from analytics; --error, nesting aggregation functions

select cast(prod(sum(aa)) * count(1 + aa) / avg(null) over () as bigint) from analytics; --error, nesting aggregation functions

select rank() over (partition by case when aa > 5 then aa else aa + 5 end) from analytics; --TODO we don't support expressions in partition by as well group by statements

select rank() over (partition by sum(aa)) from analytics; --TODO we don't support expressions in partition by as well group by

drop table analytics;
