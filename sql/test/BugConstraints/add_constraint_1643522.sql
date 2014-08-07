create table t1(id int, name varchar(1024));
alter table t1 add constraint id_p primary key(id);

alter table t1 add constraint id_p primary key(id);

select name from keys where name = 'id_p';

drop table t1;

select name from keys where name = 'id_p';
