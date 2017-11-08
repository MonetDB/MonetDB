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
select * from functions where name ='aggr01';

-- a continuous function can be called used like any other function?
select aggr01(); #should return 0

start continuous function aggr01();
call cquery.wait(1000); #wait to be started

select aggr01(); #should return 0
pause continuous aggr01;

insert into ftmp values(1),(1);
resume continuous aggr01;

call cquery.wait(1000); #wait for processing
select aggr01(); #should return 2

pause continuous aggr01;
insert into ftmp values(2),(2);
insert into ftmp values(3),(3);

resume continuous aggr01;
call cquery.wait(1000);
select aggr01(); #should return 6

call cquery.wait(1000);
select aggr01(); #should return 6

stop continuous aggr01;
drop function aggr01;
drop table ftmp;
