create table a (x varchar(10));
insert into a values ('aaa');

create table b (x varchar(10));
insert into b values ('aaa');
insert into b values ('aAa');
insert into b values ('aA');

select a.x from a,b where a.x LIKE b.x;

drop table b;
drop table a;
