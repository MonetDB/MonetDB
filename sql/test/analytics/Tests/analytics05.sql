create table analytics (aa int, bb int, cc bigint);
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);

select count(*) over w, cast(sum(aa) over w as bigint)
    from analytics window w as (rows between 5 preceding and 0 following);
select count(*) over w, cast(sum(aa) over w as bigint)
    from analytics window w as (rows between 5 preceding and 0 following), w as (range between 5 preceding and 0 following); --error, redefinition of window w
select count(*) over w, cast(sum(aa) over w as bigint)
    from analytics; --error, definition of w does not exist

select count(*) over w, cast(sum(aa) over z as bigint), avg(aa) over z
    from analytics window w as (rows between 5 preceding and 0 following), z as (order by bb range unbounded preceding);

with helper as (select count(*) over w as counted from analytics window w as (order by bb))
    select count(*) over w from helper window w as (rows unbounded preceding);

select count(*) over w window w as ();
select (select count(*) over w window w as ());

drop table analytics;
