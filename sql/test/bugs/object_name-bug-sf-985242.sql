create table "\t" (id int);
drop table "\t";
create table " " (id int);
drop table " ";
create table x ("\t" int);
select * from x;
drop table "x";
