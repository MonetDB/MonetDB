select timestamp '2119-02-17 02:08:12.345678';

select date_trunc('microseconds', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('milliseconds', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('second', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('minute', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('hour', timestamp '2119-02-17 02:08:12.345678');

select date_trunc('day', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('week', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('month', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('quarter', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('year', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('decade', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('century', timestamp '2119-02-17 02:08:12.345678');
select date_trunc('millenium', timestamp '2119-02-17 02:08:12.345678');
