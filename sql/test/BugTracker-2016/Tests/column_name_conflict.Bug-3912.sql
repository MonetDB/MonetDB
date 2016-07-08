create table foo_bar (baz int);
create table foo (bar_baz int);
insert into foo values (1); 
insert into foo_bar values (2); 
select * from foo_bar;
drop table foo_bar;
