create function f(i integer) returns bool
begin
return true;
end;
create table t(i integer);
insert into t values(1), (2);
select f(i) from t;

drop table t;
drop function f;
