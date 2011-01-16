create table a1296395 (id int primary key);
create table b1296395 (id int, foreign key (id) references a1296395(id) on delete cascade);
insert into a1296395 values (1);
insert into b1296395 values(1);
delete from a1296395; 
-- note, the "on delete cascade" constraint should have removed the
-- single record from table b here.  Until the moment that we support
-- this, this will return one record, which is wrong.
select count(*) from b1296395;
