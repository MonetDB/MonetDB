Start transaction;
create temp table table1841759 (f1 text);
insert into table1841759 values (repeat('abcdefghijkl', 100000));
select * from table1841759;
drop table table1841759;
COMMIT;
