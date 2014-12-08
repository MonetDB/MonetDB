start transaction;
create temp table foo (f1 string, f2 text, f3 varchar(12345678));
insert into foo values('aa1','bb1','cc1');
select * from foo;
insert into foo values('aa2','bb2','cc2');
select * from foo;
insert into foo values('aa3','bb3','cc3');
select * from foo;
update foo set f1 = 'abc';
select * from foo;
delete from foo where f2 = 'bb2';
select * from foo;
insert into foo values('aa4','bb4','cc4');
select * from foo;
drop table foo;
commit;

