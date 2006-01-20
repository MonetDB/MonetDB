create table foo (bar timestamp);
commit;
insert into foo values('12:12:12:99');
rollback;
insert into foo values(timestamp '12:12:12:99');
rollback;
select * from foo;
drop table foo;
commit;
