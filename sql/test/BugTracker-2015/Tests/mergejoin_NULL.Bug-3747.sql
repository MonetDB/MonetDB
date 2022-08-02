create table foo (a string, b int);
create table bar (a string, b int);
insert into foo values ('hi',0),('there',null),('monet',1);
insert into bar values ('sup',0),('dude',1);
select foo.a,foo.b,bar.b,bar.a from foo join bar on foo.b=bar.b;
drop table foo;
drop table bar;
