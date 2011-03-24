create table p1409824 ( id int, name string, val int);
insert into p1409824 values (1, 'name1', 2); 
insert into p1409824 values (1, 'name4', 4); 
insert into p1409824 values (71, 'name7', 74); 

select * from p1409824 where 0 = 1 or name = 'name4';
select * from p1409824 where name = 'name4' or 0 = 1;

select * from env() as env where 0 = 1 or name = 'sql_debug';
select * from env() as env where name = 'sql_debug' or 0 = 1;

drop table p1409824;
