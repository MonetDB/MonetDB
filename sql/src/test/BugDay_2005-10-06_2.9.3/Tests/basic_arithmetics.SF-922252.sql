create table my_table ( att INT );
insert into my_table values (1);
insert into my_table values (2);
insert into my_table values (3);
select * from my_table;

select 1<2;
select 'A'='a';
select 1 from my_table where 1<2;
select 1 from my_table where true;
select '1' where 1 = 0;
select * from my_table where 1 = 0;
drop table my_table;
