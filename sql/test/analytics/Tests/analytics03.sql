create table rowsvsrangevsgroups (aa int, bb int, cc real);

start transaction;
insert into rowsvsrangevsgroups values (1,1,1), (2,1,2), (3,1,3), (1,2,1), (1,2,1), (1,2,1), (2,2,2), (3,2,3), (4,2,4), (2,2,2);

select cast(sum(aa) over (rows unbounded preceding) as bigint),
       cast(sum(aa) over (range unbounded preceding) as bigint),
       cast(sum(aa) over (order by aa rows unbounded preceding) as bigint),
       cast(sum(aa) over (order by aa range unbounded preceding) as bigint),
       cast(sum(aa) over (order by aa groups unbounded preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb rows unbounded preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb range unbounded preceding) as bigint),
       cast(sum(aa) over (partition by bb order by bb groups unbounded preceding) as bigint) from rowsvsrangevsgroups;

select cast(sum(aa) over (order by aa range between unbounded preceding and current row) as bigint),
       cast(sum(cc) over (order by aa range between unbounded preceding and current row) as bigint),
       count(*) over (order by aa range between unbounded preceding and current row),
       count(aa) over (order by aa range between unbounded preceding and current row),
       min(aa) over (order by aa range between unbounded preceding and current row),
       max(aa) over (order by aa range between unbounded preceding and current row),
       avg(aa) over (order by aa range between unbounded preceding and current row),
       avg(cc) over (order by aa range between unbounded preceding and current row) from rowsvsrangevsgroups;

select cast(sum(aa) over (order by aa groups between unbounded preceding and current row) as bigint),
       cast(sum(cc) over (order by aa groups between unbounded preceding and current row) as bigint),
       count(*) over (order by aa groups between unbounded preceding and current row),
       count(aa) over (order by aa groups between unbounded preceding and current row),
       min(aa) over (order by aa groups between unbounded preceding and current row),
       max(aa) over (order by aa groups between unbounded preceding and current row),
       avg(aa) over (order by aa groups between unbounded preceding and current row),
       avg(cc) over (order by aa groups between unbounded preceding and current row) from rowsvsrangevsgroups;

select cast(sum(aa) over (order by aa groups between 1 preceding and current row) as bigint),
       avg(cc) over (order by aa groups between 1 preceding and current row) from rowsvsrangevsgroups;

delete from rowsvsrangevsgroups where aa = 2;

select cast(sum(aa) over (order by aa range between unbounded preceding and current row) as bigint),
       cast(sum(cc) over (order by aa range between unbounded preceding and current row) as bigint),
       count(*) over (order by aa range between unbounded preceding and current row),
       count(aa) over (order by aa range between unbounded preceding and current row),
       min(aa) over (order by aa range between unbounded preceding and current row),
       max(aa) over (order by aa range between unbounded preceding and current row),
       avg(aa) over (order by aa range between unbounded preceding and current row),
       avg(cc) over (order by aa range between unbounded preceding and current row) from rowsvsrangevsgroups;

select cast(sum(aa) over (order by aa groups between unbounded preceding and current row) as bigint),
       cast(sum(cc) over (order by aa groups between unbounded preceding and current row) as bigint),
       count(*) over (order by aa groups between unbounded preceding and current row),
       count(aa) over (order by aa groups between unbounded preceding and current row),
       min(aa) over (order by aa groups between unbounded preceding and current row),
       max(aa) over (order by aa groups between unbounded preceding and current row),
       avg(aa) over (order by aa groups between unbounded preceding and current row),
       avg(cc) over (order by aa groups between unbounded preceding and current row) from rowsvsrangevsgroups;

select cast(sum(aa) over (order by aa groups between 1 preceding and current row) as bigint),
       avg(cc) over (order by aa groups between 1 preceding and current row) from rowsvsrangevsgroups;

create table stressme (aa varchar(64), bb int);
insert into stressme values ('one', 1), ('another', 1), ('stress', 1), (NULL, 2), ('ok', 2), ('check', 3), ('me', 3), ('please', 3), (NULL, 4);

select count(aa) over (rows between current row and unbounded following),
       count(aa) over (range between current row and unbounded following),
       count(aa) over (order by bb rows between current row and unbounded following),
       count(aa) over (order by bb range between current row and unbounded following),
       count(aa) over (order by bb groups between current row and unbounded following),
       count(aa) over (partition by bb order by bb rows unbounded preceding),
       count(aa) over (partition by bb order by bb range unbounded preceding),
       count(aa) over (partition by bb order by bb groups unbounded preceding) from stressme;

select min(aa) over (rows between current row and unbounded following),
       min(aa) over (range between current row and unbounded following),
       min(aa) over (order by bb rows between current row and unbounded following),
       min(aa) over (order by bb range between current row and unbounded following),
       min(aa) over (order by bb groups between current row and unbounded following),
       min(aa) over (partition by bb order by bb rows unbounded preceding),
       min(aa) over (partition by bb order by bb range unbounded preceding),
       min(aa) over (partition by bb order by bb groups unbounded preceding) from stressme;

select max(aa) over (rows between current row and unbounded following),
       max(aa) over (range between current row and unbounded following),
       max(aa) over (order by bb rows between current row and unbounded following),
       max(aa) over (order by bb range between current row and unbounded following),
       max(aa) over (order by bb groups between current row and unbounded following),
       max(aa) over (partition by bb order by bb rows unbounded preceding),
       max(aa) over (partition by bb order by bb range unbounded preceding),
       max(aa) over (partition by bb order by bb groups unbounded preceding) from stressme;

select max(aa) over (order by bb groups between 1 preceding and current row),
       count(aa) over (order by bb groups between 1 preceding and current row) from stressme;

delete from stressme where bb = 2;

select max(aa) over (order by bb groups between 1 preceding and current row),
       count(aa) over (order by bb groups between 1 preceding and current row) from stressme;

create table overflowme (aa int, bb int);
insert into overflowme values (2147483644, 1), (2147483645, 2), (2147483646, 1), (2147483644, 2), (2147483645, 1), (2147483646, 2);

select floor(avg(aa) over (rows between current row and unbounded following)),
       floor(avg(aa) over (range between current row and unbounded following)),
       floor(avg(aa) over (order by bb rows between current row and unbounded following)),
       floor(avg(aa) over (order by bb range between current row and unbounded following)),
       floor(avg(aa) over (partition by bb order by bb rows unbounded preceding)),
       floor(avg(aa) over (partition by bb order by bb range unbounded preceding)) from overflowme;

rollback;

select count(*) over (rows between NULL preceding and unbounded following) from rowsvsrangevsgroups; --error
select count(*) over (rows between unbounded preceding and -1 following) from rowsvsrangevsgroups; --error
select count(*) over (range between 1 preceding and unbounded following) from rowsvsrangevsgroups; --error
select count(*) over (range between unbounded preceding and 1 following) from rowsvsrangevsgroups; --error
select count(*) over (groups between 1 preceding and 1 following) from rowsvsrangevsgroups; --error
select count(*) over (groups between current row and unbounded following) from rowsvsrangevsgroups; --error

drop table rowsvsrangevsgroups;
