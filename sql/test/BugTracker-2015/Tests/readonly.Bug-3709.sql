create table s10 (x int); 
start transaction; 
insert into s10 values (8); 
alter table s10 set read only; 
trace select * from s10; 
rollback;
drop table s10; 
