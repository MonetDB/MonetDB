start transaction;
create table dt_tmp( t timestamp);
insert into dt_tmp values (timestamp '2119-02-17 02:08:12.345678'), (null);

select * from dt_tmp;

select date_trunc('microseconds', t) from dt_tmp;
select date_trunc('milliseconds', t) from dt_tmp;
select date_trunc('second', t) from dt_tmp;
select date_trunc('minute', t) from dt_tmp;
select date_trunc('hour', t) from dt_tmp;

select date_trunc('day', t) from dt_tmp;
select date_trunc('week', t) from dt_tmp;
select date_trunc('month', t) from dt_tmp;
select date_trunc('quarter', t) from dt_tmp;
select date_trunc('year', t) from dt_tmp;
select date_trunc('decade', t) from dt_tmp;
select date_trunc('century', t) from dt_tmp;
select date_trunc('millenium', t) from dt_tmp;
rollback;
