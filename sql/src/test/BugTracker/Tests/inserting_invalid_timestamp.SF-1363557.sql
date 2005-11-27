create table foo (bar timestamp);
insert into foo values('12:12:12:99');
insert into foo values(timestamp '12:12:12:99');
select * from foo;
drop table foo;
