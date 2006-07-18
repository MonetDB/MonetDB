create function my_abs(v int) returns int as 
begin
	if v < 0 then 
		return -v;
	else
		return v;
	end if;
end;

select my_abs(-1);
select my_abs(1);
