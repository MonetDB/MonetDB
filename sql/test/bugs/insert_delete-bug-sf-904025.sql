create table tmp(name string, id int);
insert into tmp select name, id from tables where "type" = 0;
select count(*) from tmp;
delete from tmp;
select count(*) from tmp;
insert into tmp select name, id from tables where "type" = 0;
select count(*) from tmp;
delete from tmp;
select count(*) from tmp;

drop table tmp;
