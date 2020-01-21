-- Postgresl 11 date functions from 9.9.1

select date '2001-09-28' + integer '7';
-- date '2001-10-05'

select date '2001-09-28' + interval '1 hour';
-- timestamp '2001-09-28 01:00:00'

select date '2001-09-28' + time '03:00';
-- timestamp '2001-09-28 03:00:00'

select interval '1 day' + interval '1 hour';
-- interval '1 day 01:00:00'

select timestamp '2001-09-28 01:00' + interval '23 hours';
-- timestamp '2001-09-29 00:00:00'

select time '01:00' + interval '3 hours';
--  	time '04:00:00'

select 	- interval '23 hours';
--interval '-23:00:00'

select  	date '2001-10-01' - date '2001-09-28';
-- integer '3' (days)

select	date '2001-10-01' - integer '7';
-- 	date '2001-09-24'

select  	date '2001-09-28' - interval '1 hour';
--timestamp '2001-09-27 23:00:00'

select  	time '05:00' - time '03:00';
-- 	interval '02:00:00'

select time '05:00' - interval '2 hours';
--time '03:00:00'

select timestamp '2001-09-28 23:00' - interval '23 hours';
-- timestamp '2001-09-28 00:00:00'

select interval '1 day' - interval '1 hour';
--  	interval '1 day -01:00:00'

select timestamp '2001-09-29 03:00' - timestamp '2001-09-27 12:00';
--interval '1 day 15:00:00'

select 900 * interval '1 second';
--interval '00:15:00'

select  	21 * interval '1 day';
-- interval '21 days'

select double precision '3.5' * interval '1 hour';
--  	interval '03:30:00'

select interval '1 hour' / double precision '1.5';
-- interval '00:40:00'

select isfinite(date '2001-02-16');
select isfinite(timestamp '2001-02-16 21:28:30');
select  	isfinite(interval '4 hours');

select justify_days(interval '35 days');
-- 1 mon 5 days

select justify_hours(interval '27 hours');
-- 1 day 03:00:00

select justify_interval(interval '1 mon -1 hour');
-- 29 days 23:00:00

select statement_timestamp();
select timeofday();

-- date_part(text, timestamp)
-- date_part(text, interval)
select transaction_timestamp();


select make_date(2013, 7, 15);
select make_time(8, 15, 23.5);
select make_timestamp(2013, 7, 15, 8, 15, 23.5);
select make_timestamptz(2013, 7, 15, 8, 15, 23.5);
select make_interval(days => 10);
