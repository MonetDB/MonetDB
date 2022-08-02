create table A (a varchar(10));
create view myview
as select * from A;

alter table myview alter column a set null;

drop view myview;
drop table a;
