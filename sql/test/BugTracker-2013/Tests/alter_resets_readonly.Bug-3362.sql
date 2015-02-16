create table x(a int);
insert into x values (1),(2),(3);
alter table x set read only;
select (access = 1) from sys.tables where name='x';
alter table x add primary key (a);
select (access = 1) from sys.tables where name='x';
drop table x;

