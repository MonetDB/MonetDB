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

insert into savepointtest values(7);
insert into savepointtest values(8);
insert into savepointtest values(9);

savepoint name3;
select * from savepointtest;

release savepoint name1;
select * from savepointtest;
rollback to savepoint name1;
select * from savepointtest;
commit;



