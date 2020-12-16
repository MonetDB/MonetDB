start transaction;

create table savepointtest ( id int, primary key(id));

select * from savepointtest;

savepoint name1;

insert into savepointtest values(1), (2), (3);

select * from savepointtest;

savepoint name2;

insert into savepointtest values(4), (5), (6);

select * from savepointtest;

rollback to savepoint name2;

select * from savepointtest;

insert into savepointtest values(7), (8), (9);

savepoint name3;

select * from savepointtest;

rollback to savepoint name1;

select * from savepointtest;

commit;

select * from savepointtest;

select 'test';

start transaction;

select * from savepointtest;

insert into savepointtest values(10), (11), (12);

savepoint name4;

select * from savepointtest;

insert into savepointtest values(13), (14), (15);

savepoint name5;

select * from savepointtest;

release savepoint name4;

select * from savepointtest;

commit;

start transaction;

select * from savepointtest;

drop table savepointtest;

commit;

select * from savepointtest;

