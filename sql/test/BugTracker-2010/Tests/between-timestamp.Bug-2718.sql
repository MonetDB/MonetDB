start transaction;

create table bug2718 (time timestamp, val int);
insert into bug2718 values (timestamp '2010-11-17 13:37:55', 1);
insert into bug2718 values (timestamp '2010-11-17 13:37:56', 2);
insert into bug2718 values (timestamp '2010-11-17 13:37:57', 3);
select * from bug2718;
select t1.time, count(t2.val)
from bug2718 t1, bug2718 t2
where t2.time between (t1.time - interval '1' second) and (t1.time + interval '1' second)
group by t1.time
order by t1.time;

rollback;
