create table rr (id int);
insert into rr values (1),(2),(3);
prepare select * from rr limit ?;
exec 0 (1);

