start transaction;
create table updating (a int);
insert into updating values (1), (2);
update updating set a = 3 where a = 2;
select a from updating where a = 3;
rollback;

create table updating (a int);
insert into updating values (2);
start transaction;
update updating set a = 3 where a = 2;
select a from updating where a = 3;
rollback;

truncate updating;
insert into updating values (1), (1);
start transaction;
update updating set a = 3 where a = 2;
select a from updating where a = 3;
rollback;

truncate updating;
insert into updating values (1), (2);
start transaction;
update updating set a = 3 where a = 2;
select a from updating where a = 3;
rollback;
drop table updating;
