create table wordindex (position int, term varchar(100));
insert into wordindex values (2, "a");
insert into wordindex values (3, "b");
insert into wordindex values (6, "b");
insert into wordindex values (7, "c");
insert into wordindex values (10, "c");
insert into wordindex values (11, "d");

select * from (select * from wordindex) as x;

