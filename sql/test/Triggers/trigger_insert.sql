
create table t1 (id int, name varchar(1024));
create table t2 (id int);

create trigger test1
	after insert on t1 referencing new row as ins
	for each statement insert into t2 values( ins.id );

insert into t1 values (1, 'testing');

select * from t1;
select * from t2;

drop trigger test1;

--Cleanup
drop table t1;
drop table t2;
