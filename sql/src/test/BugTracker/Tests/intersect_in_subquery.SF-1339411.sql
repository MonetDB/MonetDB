
create table urls ( urlid int, a int, b int ); 
insert into urls values (1, 99, 199);
insert into urls values (2, 99, 299);

select urlid from urls where a < 100 and b <200;

((select urlid from urls where a < 100 and b <200)
intersect
(select urlid from urls where a < 100 and b <300));

select * from urls where urlid in (select urlid from urls where urlid<10);

select * from urls where urlid in
	((select urlid from urls where a < 100 and b <200)
	intersect
	(select urlid from urls where a < 100 and b <300));

drop table urls;
