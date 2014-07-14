create table x(a int);
insert into x values (1),(2),(3);
alter table x set read only;
select readonly from sys.tables where name='x';
alter table x add primary key (a);
select readonly from sys.tables where name='x';
drop table x;

