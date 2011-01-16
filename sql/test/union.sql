create table uniontmp(i int, s string);
insert into uniontmp values(1,'hello');
insert into uniontmp values(2,'world');
select * from 
	( select * from uniontmp union all select * from uniontmp) as a;

select * from 
	( select * from uniontmp union select * from uniontmp) as a;

select * from 
	( select * from uniontmp union distinct select * from uniontmp) as a;
drop table uniontmp;
