
create table x (n int, s varchar(10));
insert into x values (1, 'one');
insert into x values (2, 'two');
insert into x values (3, 'three');

-- swap the two attributes
create view x1 as
select s as a1, n as a2 from x;

-- swap them again
create view x2 as
select a2 as a1, a1 as a2 from x1;

-- select on the original 's' attribute fails
select * from x2 where a2='two';


drop view x2;
drop view x1;
drop table x;

