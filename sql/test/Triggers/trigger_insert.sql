create table t_8_1 (id int, name varchar(1024));
create table t_8_2 (id int);

create trigger test_8_1
	after insert on t_8_1 referencing new row as ins
	for each statement insert into t_8_2 values( ins.id );

insert into t_8_1 values (1, 'testing');

select * from t_8_1;
select * from t_8_2;

drop trigger test_8_1;

--Cleanup
drop table t_8_1;
drop table t_8_2;
