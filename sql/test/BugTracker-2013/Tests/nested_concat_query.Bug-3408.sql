declare datem integer;
declare dateh integer;
declare dated integer;
declare stamp varchar(32);
select stamp;

/*
set datem = right(concat('00',cast(EXTRACT(MINUTE FROM localtimestamp()) as varchar(2))), 2);
set dateh = right(concat('00',cast(EXTRACT(HOUR FROM localtimestamp()) as varchar(2))), 2);
set dated = right(concat('00',cast(EXTRACT(DAY FROM localtimestamp()) as varchar(2))), 2);
*/

set datem = '12';
set dateh = '11';
set dated = '10';

set stamp = concat(concat(concat(concat(concat(concat('2013-11-', dated), ' '), dateh), ':'), datem), ':30.000000');

select stamp;

set stamp = concat('2013-11-', dated);
set stamp = concat(stamp, ' ');
set stamp = concat(stamp, dateh);
set stamp = concat(stamp, ':');
set stamp = concat(stamp, datem);
set stamp = concat(stamp, ':01.000000');

select stamp;
