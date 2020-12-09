start transaction;

create table savepointtest (id int, primary key(id));
savepoint name1;

insert into savepointtest values(1), (2), (3);
select * from savepointtest;
savepoint name2;

insert into savepointtest values(4), (5), (6);
insert into savepointtest values(7), (8), (9);
select * from savepointtest;
savepoint name3;

release savepoint name1;
select * from savepointtest;
commit;

