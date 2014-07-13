
create function truncode(lon decimal(9,6), lat decimal(9,6))
   returns int 
begin
	return 1;
end;

select truncode(12,12); 
select truncode(12.0,12);
select truncode(12.00,12);

drop function truncode;
