create table rankbug (id int);
insert into rankbug values (42);
insert into rankbug select * from rankbug;
insert into rankbug select * from rankbug;
insert into rankbug select * from rankbug;
insert into rankbug select * from rankbug;
insert into rankbug select * from rankbug;
select RANK () OVER () as foo from rankbug;
drop table rankbug;
