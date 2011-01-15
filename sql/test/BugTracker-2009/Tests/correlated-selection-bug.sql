
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

select 
	"store"."store_type" as "c0", 
	count(distinct (
	select 
		"warehouse_class"."warehouse_class_id" AS "warehouse_class_id" 
	from
		"warehouse_class" AS "warehouse_class" 
	where
		"warehouse_class"."warehouse_class_id" = "warehouse"."warehouse_class_id" and 
		"warehouse_class"."description" = 'Large Owned')) as "m0",
	count(distinct (
	select 
		"warehouse_class"."warehouse_class_id" AS "warehouse_class_id" 
	from 
		"warehouse_class" AS "warehouse_class" 
	where
		"warehouse_class"."warehouse_class_id" = "warehouse"."warehouse_class_id" and 
		"warehouse_class"."description" = 'Large Independent')) as "m1",
	count((
	select 
		"warehouse_class"."warehouse_class_id" AS "warehouse_class_id" 
	from 
		"warehouse_class" AS "warehouse_class"
 	where
		"warehouse_class"."warehouse_class_id" = "warehouse"."warehouse_class_id" and 
		"warehouse_class"."description" = 'Large Independent')) as "m2",
	count(distinct "store_id"+"warehouse_id") as "m3",
	count("store_id"+"warehouse_id") as "m4", 
	count("warehouse"."stores_id") as "m5" 
from 
	"store" as "store", 
	"warehouse" as "warehouse" 
where
	"warehouse"."stores_id" = "store"."store_id" 
group by 
	"store"."store_type";

drop table store;
drop table warehouse;
drop table warehouse_class;

