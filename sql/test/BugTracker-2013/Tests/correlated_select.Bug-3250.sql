
start transaction;
create table mytest (
	id int,
	n int,
	flag int
);

insert into mytest values (1,1,null), 
	(2 ,    1 , null ),
	(3 ,    1 , null ),
	(4 ,    2 , null ),
	(5 ,    1 ,   42 ),
	(6 ,    1 ,   42 );
select * from mytest;

select id, n, flag, (select count(*) from mytest as i where i.id <
	mytest.id and i.n = mytest.n) from mytest order by id;

select id, n, flag, (select count(*) from mytest as i where i.id <
	mytest.id and i.n = mytest.n and flag is not null) from mytest order by id;
rollback;
