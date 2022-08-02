
CREATE TABLE "sys"."store" ( 
        "store_id" int NOT NULL, 
        "store_type" varchar(30), 
        "region_id" int, 
        "store_name" varchar(30), 
        "store_number" int, 
        "store_street_address" varchar(30), 
        "store_city" varchar(30), 
        "store_state" varchar(30), 
        "store_postal_code" varchar(30), 
        "store_country" varchar(30), 
        "store_manager" varchar(30), 
        "store_phone" varchar(30), 
        "store_fax" varchar(30), 
        "first_opened_date" timestamp(7), 
        "last_remodel_date" timestamp(7), 
        "store_sqft" int, 
        "grocery_sqft" int, 
        "frozen_sqft" int, 
        "meat_sqft" int, 
        "coffee_bar" smallint, 
        "video_store" smallint, 
        "salad_bar" smallint, 
        "prepared_food" smallint, 
        "florist" smallint 
);

CREATE TABLE "sys"."warehouse" (
        "warehouse_id" int NOT NULL,
        "warehouse_class_id" int,
        "stores_id" int,
        "warehouse_name" varchar(60),
        "wa_address1" varchar(30),
        "wa_address2" varchar(30),
        "wa_address3" varchar(30),
        "wa_address4" varchar(30),
        "warehouse_city" varchar(30),
        "warehouse_state_province" varchar(30),
        "warehouse_postal_code" varchar(30),
        "warehouse_country" varchar(30),
        "warehouse_owner_name" varchar(30),
        "warehouse_phone" varchar(30),
        "warehouse_fax" varchar(30)
);

CREATE TABLE "sys"."warehouse_class" (
        "warehouse_class_id" int NOT NULL,
        "description" varchar(30)
);

CREATE TABLE "sys"."inventory_fact_1997" (
	"product_id" int NOT NULL,
	"time_id" int,
	"warehouse_id" int,
	"store_id" int,
	"units_ordered" int,
	"units_shipped" int,
	"warehouse_sales" decimal(10,4),
	"warehouse_cost" decimal(10,4),
	"supply_time" smallint,
	"store_invoice" decimal(10,4)
);

CREATE TABLE "sys"."time_by_day" (
	"time_id" int NOT NULL,
	"the_date" timestamp(7),
	"the_day" varchar(30),
	"the_month" varchar(30),
	"the_year" smallint,
	"day_of_month" smallint,
	"week_of_year" int,
	"month_of_year" smallint,
	"quarter" varchar(30),
	"fiscal_period" varchar(30)
);

select 
	"warehouse"."warehouse_country" as "c0", 
       	"time_by_day"."the_year" as "c1", 
       	"time_by_day"."quarter" as "c2", 
       	"time_by_day"."month_of_year" as "c3" 
  from 
	"warehouse" as "warehouse", 
       	"inventory_fact_1997" as "inventory_fact_1997", 
       	"time_by_day" as "time_by_day" 
where
       "inventory_fact_1997"."warehouse_id" = "warehouse"."warehouse_id" and
       "inventory_fact_1997"."time_id" = "time_by_day"."time_id" 
group by
       "warehouse"."warehouse_country", 
	"time_by_day"."the_year",
       "time_by_day"."quarter", 
	"time_by_day"."month_of_year" 
order by 1 ASC, 2 ASC, 3 ASC, 4 ASC;

DROP TABLE "sys"."store"; 
DROP TABLE "sys"."warehouse";
DROP TABLE "sys"."warehouse_class";
DROP TABLE "sys"."inventory_fact_1997";
DROP TABLE "sys"."time_by_day";
