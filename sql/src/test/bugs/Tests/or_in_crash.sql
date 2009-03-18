CREATE TABLE "sys"."product" (
        "product_class_id"   int        NOT NULL,
        "product_id"         int        NOT NULL,
        "brand_name"         varchar (60),
        "product_name"       varchar (60)       NOT NULL,
        "SKU"                bigint     NOT NULL,
        "SRP"                decimal (10,4),
        "gross_weight"       real    ,
        "net_weight"         real    ,
        "recyclable_package" smallint,
        "low_fat"            smallint,
        "units_per_case"     smallint,
        "cases_per_pallet"   smallint,
        "shelf_width"        real    ,
        "shelf_height"       real    ,
        "shelf_depth"        real    
);
CREATE INDEX "i_prod_brand_name" ON "product" ("brand_name");
CREATE INDEX "i_prod_class_id" ON "product" ("product_class_id");
CREATE INDEX "i_product_SKU" ON "product" ("SKU");
CREATE INDEX "i_product_id" ON "product" ("product_id");
CREATE INDEX "i_product_name" ON "product" ("product_name");

CREATE TABLE "sys"."product_class" (
        "product_class_id"    int       NOT NULL,
        "product_subcategory" varchar(30),
        "product_category"    varchar(30),
        "product_department"  varchar(30),
        "product_family"      varchar(30)
);

CREATE TABLE "sys"."sales_fact_1997" (
        "product_id"   int      NOT NULL,
        "time_id"      int      NOT NULL,
        "customer_id"  int      NOT NULL,
        "promotion_id" int      NOT NULL,
        "store_id"     int      NOT NULL,
        "store_sales"  decimal(10,4)    NOT NULL,
        "store_cost"   decimal(10,4)    NOT NULL,
        "unit_sales"   decimal(10,4)    NOT NULL
);
CREATE INDEX "i_sls_97_cust_id" ON "sales_fact_1997" ("customer_id");
CREATE INDEX "i_sls_97_prod_id" ON "sales_fact_1997" ("product_id");
CREATE INDEX "i_sls_97_promo_id" ON "sales_fact_1997" ("promotion_id");
CREATE INDEX "i_sls_97_store_id" ON "sales_fact_1997" ("store_id");
CREATE INDEX "i_sls_97_time_id" ON "sales_fact_1997" ("time_id");

CREATE TABLE "sys"."customer" (
        "customer_id"          int      NOT NULL,
        "account_num"          bigint   NOT NULL,
        "lname"                varchar (30)     NOT NULL,
        "fname"                varchar (30)     NOT NULL,
        "mi"                   varchar (30),
        "address1"             varchar (30),
        "address2"             varchar (30),
        "address3"             varchar (30),
        "address4"             varchar (30),
        "city"                 varchar (30),
        "state_province"       varchar (30),
        "postal_code"          varchar (30)     NOT NULL,
        "country"              varchar (30)     NOT NULL,
        "customer_region_id"   int      NOT NULL,
        "phone1"               varchar (30)     NOT NULL,
        "phone2"               varchar (30)     NOT NULL,
        "birthdate"            date     NOT NULL,
        "marital_status"       varchar (30)     NOT NULL,
        "yearly_income"        varchar (30)     NOT NULL,
        "gender"               varchar (30)     NOT NULL,
        "total_children"       smallint NOT NULL,
        "num_children_at_home" smallint NOT NULL,
        "education"            varchar (30)     NOT NULL,
        "date_accnt_opened"    date     NOT NULL,
        "member_card"          varchar (30),
        "occupation"           varchar (30),
        "houseowner"           varchar (30),
        "num_cars_owned"       int     ,
        "fullname"             varchar (60)     NOT NULL
);
CREATE INDEX "i_cust_acct_num" ON "customer" ("account_num");
CREATE INDEX "i_cust_child_home" ON "customer" ("num_children_at_home");
CREATE INDEX "i_cust_postal_code" ON "customer" ("postal_code");
CREATE INDEX "i_cust_region_id" ON "customer" ("customer_region_id");
CREATE INDEX "i_customer_fname" ON "customer" ("fname");
CREATE INDEX "i_customer_id" ON "customer" ("customer_id");
CREATE INDEX "i_customer_lname" ON "customer" ("lname");

select 
	"product_class"."product_family" as "c0",
	"product_class"."product_department" as "c1", 
	"product_class"."product_category" as "c2", 
	"customer"."education" as "c3" 
from 
	"product" as "product",
	"product_class" as "product_class", 
	"sales_fact_1997" as "sales_fact_1997", 
	"customer" as "customer" 
where 
	"product"."product_class_id" = "product_class"."product_class_id" and 
	"sales_fact_1997"."product_id" = "product"."product_id" and
	"sales_fact_1997"."customer_id" = "customer"."customer_id" and
	(("product_class"."product_department" = 'Alcoholic Beverages' and 
		"product_class"."product_family" = 'Drink' and
		"product_class"."product_category" = 'Beer and Wine') or
	 ("product_class"."product_department" = 'Beverages' and 
	  	"product_class"."product_family" = 'Drink' and 
		"product_class"."product_category" in 
		('Carbonated Beverages', 'Drinks', 'Hot Beverages' , 'Pure Juice Beverages')) or 
		("product_class"."product_department" = 'Dairy' and 
		"product_class"."product_family" = 'Drink' and
	"product_class"."product_category" = 'Dairy')) 
group by 
	"product_class"."product_family",
	"product_class"."product_department", 
	"product_class"."product_category",
	"customer"."education" 
order by 
	"product_class"."product_family" ASC,
	"product_class"."product_department" ASC, 
	"product_class"."product_category" ASC, 
	"customer"."education" ASC;
