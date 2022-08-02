create schema tempo;

create temporary table temp.dummy(i int);

create temporary table tempo.dummy(i int);
select * from tempo.dummy;
select * from tmp.dummy;

drop table tmp.dummy;
drop schema tempo;
