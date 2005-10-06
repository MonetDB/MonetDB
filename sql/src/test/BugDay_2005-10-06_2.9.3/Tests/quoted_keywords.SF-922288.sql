create table tab ( att int not null, "null" int not null);
insert into tab values (1,2);
insert into tab values (3,4);
select * from tab;

SELECT null AS test, "null" AS nullable FROM tab;
