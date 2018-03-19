set time zone interval '+02:00' hour to minute;

create table my_dates(dt date, ts timestamp, tstz timestamp with time zone);
insert into my_dates values (date '2017-06-14', timestamp '2017-06-14 12:12:12.12', timestamp with time zone '2017-06-14 12:12:12.12 CET+1');
insert into my_dates values (date '2016-07-14', timestamp '2016-07-14 12:12:12.12', timestamp with time zone '2016-07-14 12:12:12.12 CET+1');
insert into my_dates values (date '2014-01-14', timestamp '2014-01-14 12:12:12.12', timestamp with time zone '2014-01-14 12:12:12.12 CET+1');
insert into my_dates values (date '2015-12-14', timestamp '2015-12-14 12:12:12.12', timestamp with time zone '2015-12-14 12:12:12.12 CET+1');

select * from my_dates order by dt;

select dt, extract(year from dt) as dt_yr from my_dates order by dt;
select ts, extract(year from ts) as ts_yr from my_dates order by dt;
select tstz, extract(year from tstz) as tstz_yr from my_dates order by dt;

select dt, extract(quarter from dt) as dt_qrfrom from my_dates order by dt;
select ts, extract(quarter from ts) as ts_qrfrom from my_dates order by dt;
select tstz, extract(quarter from tstz) as tstz_qr from my_dates order by dt;

select dt, extract(week from dt) as dt_wk from my_dates order by dt;
select ts, extract(week from ts) as ts_wk from my_dates order by dt;
select tstz, extract(week from tstz) as tstz_wk from my_dates order by dt;

select dt, "year"(dt) as dt_yr from my_dates order by dt;
select ts, "year"(ts) as ts_yr from my_dates order by dt;
select tstz, "year"(tstz) as tstz_yr from my_dates order by dt;

select dt, quarter(dt) as dt_qrfrom from my_dates order by dt;
select ts, quarter(ts) as ts_qrfrom from my_dates order by dt;
select tstz, quarter(tstz) as tstz_qr from my_dates order by dt;

select dt, week(dt) as dt_wk from my_dates order by dt;
select ts, week(ts) as ts_wk from my_dates order by dt;
select tstz, week(tstz) as tstz_wk from my_dates order by dt;

drop table my_dates;

