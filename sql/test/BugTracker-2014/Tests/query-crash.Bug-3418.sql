start transaction;

create table DailyData (
sym varchar(32),
businessdate date,
closePrice float,
adjustedClosePrice float,
openingPrice float,
accVol float,
unique(businessdate, sym)
);

with r as (select t.businessdate, t.sym, 1 - t.adjustedcloseprice / y.adjustedcloseprice as ret from DailyData t
 left outer join DailyData y on t.businessdate = y.businessdate + (interval '1' day) and t.sym = y.sym
 where t.businessdate > '2013-06-01'),
v as (select sym, stddev_samp(ret) as volatility from r group by sym)
select * from v where v.sym like '%.L' limit 100;

with dd as (select * from DailyData where businessdate > '2013-06-01'),
r as (select t.businessdate, t.sym, 1 - t.adjustedcloseprice / y.adjustedcloseprice as ret from dd t
left outer join dd y on t.businessdate = y.businessdate + (interval '1' day) and t.sym = y.sym),
v as (select sym, stddev_samp(ret) as volatility from r group by sym)
select * from v where v.sym like '%.L' limit 100;

rollback;
