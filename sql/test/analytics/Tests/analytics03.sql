start transaction;

create table rowsvsrange (aa int, bb int, cc real);
insert into rowsvsrange values (1,1,1), (2,1,2), (3,1,3), (1,2,1), (1,2,1), (1,2,1), (2,2,2), (3,2,3), (4,2,4), (2,2,2);

select cast(sum(aa) over (rows unbounded preceding) as bigint),
       cast(sum(aa) over (range unbounded preceding) as bigint),
       cast(sum(aa) over (order by aa rows unbounded preceding) as bigint),
       cast(sum(aa) over (order by aa range unbounded preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb rows unbounded preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb range unbounded preceding) as bigint) from rowsvsrange;

select sum(cc) over (rows unbounded preceding) as somes,
       sum(cc) over (range unbounded preceding),
       sum(cc) over (order by cc rows unbounded preceding),
       sum(cc) over (order by cc range unbounded preceding),
       sum(cc) over (partition by bb order by bb rows unbounded preceding),
       sum(cc) over (partition by bb order by bb range unbounded preceding) from rowsvsrange order by somes;

select cast(sum(aa) over (order by aa range between current row and unbounded following) as bigint) from rowsvsrange;
select sum(cc) over (order by cc range between current row and unbounded following) from rowsvsrange;

rollback;
