create table t (tm time);
insert into t values ('20:04:04.847');
select tm from t;
select tm + interval '0' second from t;
select tm + interval '0.333' second(5) from t;
select time '20:04:04.847' - time '20:04:04.947';
select time(5) '20:04:04.947' - time(5) '20:04:04.847';
drop table t;

