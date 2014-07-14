create function MyFunc1()
returns int
begin
	  declare table t (a int);
	  insert into t values (1);
	  update t set i = 0;
	  return 0;
end;

select MyFunc1();

drop function MyFunc1;
