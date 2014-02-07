create table rr (id int);
insert into rr values (1),(2),(3);
prepare select * from rr limit ?;
exec ** (1);

drop table rr;

prepare select name, schema_id, query, type, system, commit_action, readonly, temporary from tables limit 42;
exec ** ();
