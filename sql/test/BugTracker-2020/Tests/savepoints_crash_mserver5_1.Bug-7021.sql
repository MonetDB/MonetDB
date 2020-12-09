-- triggers a SIGSEGV in sql/storage/store.c:185: if (--(i->base.refcnt) > 0)
start transaction;
create table savepointtest (id int, primary key(id));
savepoint name1;
insert into savepointtest values(1), (2), (3);
savepoint name2;
insert into savepointtest values(4), (5), (6);
insert into savepointtest values(7), (8), (9);
--savepoint name3;
select * from savepointtest;
commit;

select 42;
select * from savepointtest;

