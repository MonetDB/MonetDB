create table rr (id int);
insert into rr values (1),(2),(3);
prepare select * from rr limit ?;
exec 2 (1);

drop table rr;
