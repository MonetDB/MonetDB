create table analytics (aa int, bb int);
insert into analytics values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

start transaction;
select first_value(aa) over (w order by bb) from analytics window w as (partition by bb);

select first_value(aa) over (w1 order by bb),
       last_value(aa) over (w2 order by bb) from analytics window w1 as (w2), w2 as (), w3 as (w1);

select first_value(aa) over (w1 partition by bb),
       last_value(aa) over (w2 range unbounded preceding),
       count(aa) over (w3) from analytics window w1 as (w2), w2 as (order by bb), w3 as (w2 partition by bb);
rollback;

select first_value(aa) over (w1 order by bb) from analytics; --error, no window w1 specification

select first_value(aa) over (w1 order by aa),
       last_value(aa) over (w2) from analytics window w1 as (w2), w2 as (order by bb); --error, redefinition of order by clause

select first_value(aa) over (w1 order by bb),
       last_value(aa) over (w2 order by bb) from analytics window w1 as (w2), w2 as (w3), w3 as (w1); --error, cyclic definition

drop table analytics;
