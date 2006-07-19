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

create function my_while(v int) returns int as 
begin
	while v > 0 do 
		set v = v - 10;
	end while;
	return v;
end;

select my_while(105);
