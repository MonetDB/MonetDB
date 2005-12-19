create table a1296395 (id int primary key);
create table b1296395 (id int, foreign key (id) references a1296395(id) on delete cascade);
insert into a1296395 values (1);
insert into b1296395 values(1);
delete from a1296395; 
select * from b1296395;
