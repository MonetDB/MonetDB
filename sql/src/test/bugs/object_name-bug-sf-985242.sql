create table "\t" (id int);
rollback;

create table " " (id int);
rollback;

create table x ("\t" int);
select * from x;
