-- continues functions
create stream table ftmp(i integer) set window 2;

create function aggr01()     
returns integer
begin
    declare s int;
	set s = 0;
	while (true)
	do
		set s = s + (select count(*) from ftmp);
		yield s ; 
	end while;
	return s;
END;

select result from tmp.aggr01; #error

start continuous function aggr01();
call cquery.wait(1000); #wait to be started

select result from tmp.aggr01; #should be empty
pause continuous aggr01;

insert into ftmp values(1),(1);
resume continuous aggr01;

call cquery.wait(1000); #wait for processing
select result from tmp.aggr01; #should return 2

pause continuous aggr01;
insert into ftmp values(2),(2);
insert into ftmp values(3),(3);

resume continuous aggr01;
call cquery.wait(1000);
select result from tmp.aggr01; #should return 2,4,6

call cquery.wait(1000);
select result from tmp.aggr01; #should return 2,4,6

stop continuous aggr01;
drop function aggr01;
drop table ftmp;
