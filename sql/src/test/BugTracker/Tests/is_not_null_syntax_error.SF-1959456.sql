create function f1()
RETURNS int
BEGIN
return 0;
END;

create table t1 (id int);
select id from t1 where f1() IS NOT NULL;

drop function f1;
drop table t1;
