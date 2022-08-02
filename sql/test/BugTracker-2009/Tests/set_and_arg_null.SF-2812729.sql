declare a int;
set a = null;
select a;

create function call_function( aa int ) RETURNS int
begin
	declare b int;
	set b = aa;
	return b;
end;

select call_function(NULL);

drop function call_function;
