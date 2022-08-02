create table x (f float);
insert into x values (3.14);
select f/f from x;

select cast(3.14/3.14 as integer);
select cast(f/f as integer) from x;
drop table x;
