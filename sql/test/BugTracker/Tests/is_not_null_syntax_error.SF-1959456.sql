create function f11959456()
RETURNS int
BEGIN
return 0;
END;

create table t11959456 (id int);
select id from t11959456 where f11959456() IS NOT NULL;

drop function f11959456;
drop table t11959456;
