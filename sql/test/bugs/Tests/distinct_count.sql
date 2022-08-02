
create table tabool(id int, b boolean);
insert into tabool values(1,true);
insert into tabool values(1,true);
insert into tabool values(2,false);
select distinct count(*) from tabool;
drop table tabool;
