create table tmp (data numeric default 5);
alter table tmp alter column data drop default;

alter table tmp alter column data set default 6;
drop table tmp;

