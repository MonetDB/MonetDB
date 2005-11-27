select 1/0;

create table a (x int, y int);
insert into a values (1,2);
insert into a values (3,0);

select x/y from a;
