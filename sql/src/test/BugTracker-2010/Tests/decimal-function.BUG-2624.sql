create function decimal_function(i decimal(10,2)) returns decimal(10,2)
begin return i; end;

select decimal_function(10);
