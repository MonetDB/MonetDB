create table t_not_null(a int);
alter table t_not_null add foo int not null;

insert into t_not_null values (1,1);
insert into t_not_null (a) values (1);

drop table t_not_null;
