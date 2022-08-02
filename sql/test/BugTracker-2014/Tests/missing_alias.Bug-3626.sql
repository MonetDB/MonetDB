start transaction;
create table test2 (i int, b bool);
insert into test2 values (1, true);
select cast(b as smallint), (i > 0 and b) from test2;
