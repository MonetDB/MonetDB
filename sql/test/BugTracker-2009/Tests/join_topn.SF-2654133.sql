
create table x (n int, s varchar(10));
insert into x (n,s) values (1, 'one');
insert into x (n,s) values (2, 'two');
insert into x (n,s) values (3, 'three');

select * from tables, x limit 10;

drop table x cascade;
