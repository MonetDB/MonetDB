create merge table smalltable(i integer, j integer);

alter table smalltable add table unknowntable;

drop table smalltable;

create schema mys;
set schema mys;

create merge table mys.smalltable(i integer, j integer);

create table mys.part (i integer, j integer);
insert into mys.part values(1,2);

alter table mys.smalltable add table part1;
alter table mys.smalltable add table mys.part1;

select * from mys.smalltable;


drop table mys.smalltable;
drop table mys.part;
