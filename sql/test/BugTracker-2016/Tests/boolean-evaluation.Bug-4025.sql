start transaction;

create table test3 (pid integer);
insert into test3 values (1);
select t3.pid from test3 t3 where  false and (false or false) or true;
select t3.pid from test3 t3 where  false and (false and false) or true;

rollback;
