
create table x (s string);
insert into x values('%able%');
select * from sys._tables, x where name like s;
drop table x;
