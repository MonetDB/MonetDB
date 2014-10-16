start transaction;

create table agg_error (a bool, b bool);
select max(a), b from agg_error group by a;
