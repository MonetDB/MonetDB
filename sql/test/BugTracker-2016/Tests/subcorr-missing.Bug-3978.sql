create table x( i int, j int, id int);
insert into x values (1,1,1);
insert into x values (2,2,2);
insert into x values (2,2,1);
insert into x values (2,2,2);
select id,corr(i,j) from x group by id order by id;
drop table x;
