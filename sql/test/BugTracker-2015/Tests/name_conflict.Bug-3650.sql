create table foo (id integer, bar_id integer);
create table foo_bar (id integer);
insert into foo (id, bar_id) values (1,2), (3,4);
select * from foo;
drop table foo;
