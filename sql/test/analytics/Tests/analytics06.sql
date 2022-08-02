create table testing (aa int, bb int, cc bigint);
insert into testing values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 0, NULL), (3, 0, 3), (4, 1, 4), (6, 3, 6), (8, 0, 8), (NULL, 4, NULL);

start transaction;

select count(aa) over (partition by bb), cast(75 + count(aa) over (partition by bb) as bigint) from testing where bb <> 1;

with relation as (select row_number() over () as dd, aa, bb from testing where bb <> 1)
select aa, bb, dd,
       count(aa) over (partition by bb rows between dd preceding and current row),
       count(aa) over (partition by bb rows between dd preceding and dd following),
       count(aa) over (partition by bb rows between dd + 1 preceding and dd preceding) from relation;

select first_value(aa) over (partition by bb rows between 2 preceding and 1 following),
       last_value(aa) over (partition by bb rows between 1 preceding and 1 following) from testing;

rollback;

select max(aa) over (partition by bb rows 'something' preceding) from testing; --error
select max(distinct aa) over (partition by bb) from testing; --error

select count(cc * cc) over (partition by cc * cc rows between cc * cc preceding and cc * cc preceding) from testing; --error, null value in bound frame input

delete from testing where cc is null;

select count(cc * cc) over (partition by cc * cc rows between cc * cc preceding and cc * cc preceding) from testing;

insert into testing values (-1,-1,-1);

select count(aa) over (partition by bb rows between cc - 500 preceding and cc - 500 preceding) from testing; --error, negative value in bound frame input

drop table testing;
