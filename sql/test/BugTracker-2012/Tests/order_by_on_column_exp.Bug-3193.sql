
start transaction;
CREATE TABLE "time_by_day" (
	   "time_id" INTEGER NOT NULL,
	   "the_date" TIMESTAMP(0),
	   "the_day" VARCHAR(30),
	   "the_month" VARCHAR(30),
	   "the_year" SMALLINT,
	   "day_of_month" SMALLINT,
	   "week_of_year" INTEGER,
	   "month_of_year" SMALLINT,
	   "quarter" VARCHAR(30),
	   "fiscal_period" VARCHAR(30),
	   PRIMARY KEY ("time_id","the_date")
	 );


-- expressions are not supported in group by and order by
-- select "time_by_day"."the_year" as "c0", "the_year" || '-12-31' as "c1" from "time_by_day" as "time_by_day" 
-- group by "time_by_day"."the_year", "the_year" || '-12-31'
-- order by CASE WHEN "time_by_day"."the_year" IS NULL THEN || 1 ELSE 0 END, "time_by_day"."the_year" ASC; 

-- this doesn't work because the group by and order by for c0 are different
select "time_by_day"."the_year" as "c0", "the_year" || '-12-31' as "c1" from "time_by_day" as "time_by_day" 
group by c0, c1  
order by CASE WHEN "time_by_day"."the_year" IS NULL THEN 1 ELSE 0 END, "time_by_day"."the_year" ASC;

-- this works again
select "time_by_day"."the_year" as "c0", "the_year" || '-12-31' as "c1" from "time_by_day" as "time_by_day" 
group by c0, c1  
order by CASE WHEN c0 IS NULL THEN 1 ELSE 0 END, c0 ASC;

rollback;
