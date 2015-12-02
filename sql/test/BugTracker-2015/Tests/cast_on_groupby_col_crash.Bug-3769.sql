start transaction;
create table union_a (id bigint, mytimestamp timestamp);

create table union_b (id bigint, mytimestamp timestamp);

create view union_view as select * from union_a union all select * from union_b;

select count(*), cast(mytimestamp as date) as mydate from union_view group by mydate;
Rollback;
