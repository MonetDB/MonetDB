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

create function my_declare(v int) returns int as 
begin
	declare x int;
	set x = v;
	return x;
end;

select my_declare(105);

create function my_complex_declare(v int) returns int as 
begin
	declare x, y, z int;
	declare a int, b, c varchar(10);
	set x = v;
	return x;
end;

select my_complex_declare(1);

create function my_case(v int) returns int as
begin
	case v
	when 1 then return 100;
	when 2 then return 200;
	else return -1;
	end case;
end;

select my_case(1);
select my_case(2);
select my_case(3);

create function my_searchcase(v int) returns int as
begin
	case 
	when v=1 then return 100;
	when v=2 then return 200;
	else return -1;
	end case;
end;

select my_searchcase(1);
select my_searchcase(2);
select my_searchcase(3);
