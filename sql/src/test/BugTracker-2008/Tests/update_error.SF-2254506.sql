create table table1 (col1 int,col2 int);
create table table2 (col1 int,col2 int);

insert into table1 (col1,col2) values (1,10),(2,20),(3,30);
insert into table2 (col1,col2) values (1,100),(2,200);

select * from table1;
select * from table2;

update table1 set col2=(select table2.col2 from table2 where table2.col1=table1.col1) where exists (select * from table2 where table1.col1=table2.col1);

select * from table1;

drop table table1;
drop table table2;
