-- following gave a crash when constants optimizer was set.
create table x( id serial, a varchar(1000));
drop table x;
