create function bug2969 (x decimal(7,5), y decimal(8,5)) returns decimal(15,0)
begin
return power(10,7);
end;

select bug2969(12,12);
select bug2969(12.1111,12.1111);
select bug2969(12,12);

drop function bug2969;
