/*
set datem = right(concat('00',cast(EXTRACT(MINUTE FROM localtimestamp()) as varchar(2))), 2);
set dateh = right(concat('00',cast(EXTRACT(HOUR FROM localtimestamp()) as varchar(2))), 2);
set dated = right(concat('00',cast(EXTRACT(DAY FROM localtimestamp()) as varchar(2))), 2);
*/

select concat(concat(concat(concat(concat(concat('2013-11-', '10'), ' '), '11'), ':'), '12'), ':30.000000');

create or replace function myfunc() returns varchar(32)
begin
    declare stamp varchar(32);
    set stamp = concat('2013-11-', '10');
    set stamp = concat(stamp, ' ');
    set stamp = concat(stamp, '11');
    set stamp = concat(stamp, ':');
    set stamp = concat(stamp, '12');
    set stamp = concat(stamp, ':01.000000');
    return stamp;
end;

select myfunc();
drop function myfunc;
