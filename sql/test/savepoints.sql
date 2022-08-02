start transaction;

create table savepointtest (
	id int,
	primary key(id)
);

select * from savepointtest;

savepoint name1;

insert into savepointtest values(1);
insert into savepointtest values(2);
insert into savepointtest values(3);

select * from savepointtest;

savepoint name2;

insert into savepointtest values(4);
insert into savepointtest values(5);
insert into savepointtest values(6);

select * from savepointtest;

rollback to savepoint name2;

select * from savepointtest;

insert into savepointtest values(7);
insert into savepointtest values(8);
insert into savepointtest values(9);

savepoint name3;

rollback to savepoint name1;
commit;
select * from savepointtest;
select 'test';

rollback;
start transaction;

select * from savepointtest;

insert into savepointtest values(10);
insert into savepointtest values(11);
insert into savepointtest values(12);

savepoint name4;

insert into savepointtest values(13);
insert into savepointtest values(14);
insert into savepointtest values(15);

savepoint name5;

release savepoint name4;

select * from savepointtest;

commit;
start transaction;

select * from savepointtest;

drop table savepointtest;

commit;
