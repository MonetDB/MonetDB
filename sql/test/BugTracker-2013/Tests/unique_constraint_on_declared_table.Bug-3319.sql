create function MyFunc2()
returns int
begin
	  declare table t (a int unique);
	  insert into t values (1);
	  return select count(*) from t;
end;

select MyFunc2();

drop function MyFunc2;
