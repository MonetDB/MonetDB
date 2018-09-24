start transaction;

create table tmp(t timestamp);
insert into tmp values(timestamp '2018-09-09 15:49:45.000000');
insert into tmp values(null);
insert into tmp values(timestamp '2018-09-09 15:49:45.000000');
select * from tmp;
select * from tmp where not t;

rollback;
