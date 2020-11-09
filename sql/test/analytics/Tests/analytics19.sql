start transaction;
create table analytics (aa interval second, bb interval month);
insert into analytics values (interval '15' second, interval '3' month), (interval '3' second, interval '1' month), (interval '2' second, interval '1' month), 
                             (interval '5' second, interval '3' month), (NULL, interval '2' month), (interval '3' second, interval '2' month), 
                             (interval '4' second, interval '1' month), (interval '6' second, interval '3' month), (interval '8' second, interval '2' month), (NULL, interval '4' month);

select avg(aa) over (partition by bb),
       avg(aa) over (partition by bb order by bb asc),
       avg(aa) over (partition by bb order by bb desc),
       avg(aa) over (order by bb desc) from analytics;

select avg(bb) over (partition by bb),
       avg(bb) over (partition by bb order by bb asc),
       avg(bb) over (partition by bb order by bb desc),
       avg(bb) over (order by bb desc) from analytics;

select avg(aa) over (partition by aa),
       avg(aa) over (partition by aa order by aa asc),
       avg(aa) over (partition by aa order by aa desc),
       avg(aa) over (order by aa desc) from analytics;

select avg(bb) over (partition by aa),
       avg(bb) over (partition by aa order by aa asc),
       avg(bb) over (partition by aa order by aa desc),
       avg(bb) over (order by aa desc) from analytics;

select avg(interval '1' second) over (partition by bb),
       avg(interval '1' second) over (partition by bb order by bb asc),
       avg(interval '1' month) over (partition by bb order by bb desc),
       avg(interval '1' month) over (order by bb desc) from analytics;

select avg(interval '-1' second) over (partition by bb),
       avg(interval '-1' second) over (partition by bb order by bb asc),
       avg(interval '-100' month) over (partition by bb order by bb desc),
       avg(interval '-100' month) over (order by bb desc) from analytics;

select avg(aa) over (),
       avg(bb) over (),
       avg(aa) over (),
       avg(bb) over (),
       avg(interval '1' second) over (),
       avg(interval '1' month) over () from analytics;

select avg(CAST(NULL as interval second)) over (),
       avg(CAST(NULL as interval month)) over () from analytics;

select avg(aa) over (order by bb rows between 5 preceding and 0 following),
       avg(aa) over (order by bb rows between 5 preceding and 2 following),
       avg(aa) over (partition by bb order by bb rows between 5 preceding and 0 following),
       avg(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) from analytics;

select avg(aa) over (order by bb rows 2 preceding),
       avg(aa) over (partition by bb order by bb rows 2 preceding),
       avg(aa) over (order by bb nulls last, aa nulls last rows 2 preceding) from analytics;

select avg(bb) over (order by bb groups between 5 preceding and 0 following),
       avg(bb) over (order by bb groups between 5 preceding and 2 following),
       avg(bb) over (partition by bb order by bb groups between 5 preceding and 0 following),
       avg(bb) over (partition by bb order by bb groups between 5 preceding and 2 following) from analytics;

select avg(bb) over (order by bb groups 2 preceding),
       avg(bb) over (partition by bb order by bb groups 2 preceding) from analytics;

rollback;
