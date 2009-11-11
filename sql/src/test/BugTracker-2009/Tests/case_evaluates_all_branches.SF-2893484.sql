
create table stats_2009_week_31 (
	payout_units_cpc int,
	payout_cpc int
);

insert into stats_2009_week_31 values (0,1),
	(0,2),
	(0,0),
	(0,0),
	(0,0);

SELECT CASE SUM(payout_units_cpc) WHEN 0 THEN 0 ELSE 1 END AS avg_cost_cpc FROM stats_2009_week_31; 

SELECT CASE SUM(payout_units_cpc) WHEN 0 THEN 0 ELSE cast(SUM(payout_units_cpc * payout_cpc) as numeric(12,4)) / SUM(payout_units_cpc) END AS avg_cost_cpc FROM stats_2009_week_31;

drop table stats_2009_week_31;
