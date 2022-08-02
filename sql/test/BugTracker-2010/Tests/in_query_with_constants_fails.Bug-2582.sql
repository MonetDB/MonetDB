create table skycrash(id serial, boom boolean);
create view skyview as select 1 as unknown, id from skycrash;
select * from skyview where unknown in (1, 2, 3);
drop view skyview;
drop table skycrash;
