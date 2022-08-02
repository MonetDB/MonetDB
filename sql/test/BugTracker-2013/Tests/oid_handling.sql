create table oidtable( o oid);
insert into oidtable values( 123@0);
insert into oidtable values (234);
select * from oidtable where o in (123@0,234);
drop table oidtable;
