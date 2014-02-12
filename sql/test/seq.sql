create sequence seq as integer;
create table tst (i integer, v varchar(32));
insert into tst values (next value for seq, 'testing');
select * from tst;
insert into tst values (next value for seq, 'testing');
select * from tst;
insert into tst values (next value for seq, 'testing');
select * from tst;
commit;
