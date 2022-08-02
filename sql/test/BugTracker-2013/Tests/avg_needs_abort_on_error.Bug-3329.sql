create table test_avg (a int, b int);

insert into test_avg (a,b) values (1,1);

create procedure test_avg_proc()
begin
	 insert into test_avg (a) select avg(a) from test_avg group by b;
end;

call test_avg_proc();
select * from test_avg;

drop procedure test_avg_proc;
drop test_avg;

