create table tmp(name string, id int);
insert into tmp select name, id from tables where "istable" = true;
select count(*) from tmp;
delete from tmp;
select count(*) from tmp;
insert into tmp select name, id from tables where "istable" = true;
select count(*) from tmp;
delete from tmp;
select count(*) from tmp;
