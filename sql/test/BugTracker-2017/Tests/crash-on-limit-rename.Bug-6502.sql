start transaction;
CREATE TABLE "sys"."lineitem_denormalized_first1k_sanitised" (
	"Customer_Gender"        VARCHAR(255),
	"Customer_Region"        VARCHAR(255),
	"Customer_Country"       VARCHAR(255),
	"Customer_State"         VARCHAR(255),
	"Customer_City"          VARCHAR(255),
	"Customer_Date_Of_Birth" TIMESTAMP,
	"Customer_Id"            BIGINT,
	"Product_Family"         VARCHAR(255),
	"Product_Category"       VARCHAR(255),
	"Product_Subcategory"    VARCHAR(255),
	"Product_Name"           VARCHAR(255),
	"Product_Id"             BIGINT,
	"Store_Id"               BIGINT,
	"Store_Longitude"        DOUBLE,
	"Store_Latitude"         DOUBLE,
	"Store_Name"             VARCHAR(255),
	"Store_Manager"          VARCHAR(255),
	"Store_Phone_Number"     VARCHAR(255),
	"Store_Region"           VARCHAR(255),
	"Store_Country"          VARCHAR(255),
	"Store_State"            VARCHAR(255),
	"Store_City"             VARCHAR(255),
	"Order_Date"             TIMESTAMP,
	"Year_Begin_Date"        TIMESTAMP,
	"Quarter_Begin_Date"     TIMESTAMP,
	"Month_Begin_Date"       TIMESTAMP,
	"Yyyymm"                 BIGINT,
	"Yyyymmdd"               BIGINT,
	"Ddmonyyyy"              VARCHAR(255),
	"Line_Price"             DOUBLE,
	"Line_Cost"              DOUBLE,
	"Line_Margin"            DOUBLE,
	"Line_Margin_Percent"    DOUBLE,
	"_visokio_row_id_"       BIGINT        NOT NULL,
	CONSTRAINT "\"lineitem_denormalized_first1k_sanitised\"_PK" PRIMARY KEY ("_visokio_row_id_")
);

SELECT "t4jdu"."c4jdd_store_phone_number_8958366756", 
     "t4jdu"."c4jde_line_cost__singleton__3098294", 
     "t4jdu"."c4jdf_yyyymmdd__mean__3928221090907", 
     "t4jdu"."c4jdg_line_price__sum__145489358291", 
     "t4jdu"."c4jdh_month_begin_date__min__838817", 
     "t4jdu"."c4jdi_customer_state__max__67562250", 
     "t4jdu"."c4jdo_mode_product_family" , 
     "t4jdu"."c4jdp_product_subcategory__unique_v" 
FROM   ( 
        SELECT   "t4jds"."c4jdd_store_phone_number_8958366756", 
             "t4jds"."c4jde_line_cost__singleton__3098294", 
             "t4jds"."c4jdf_yyyymmdd__mean__3928221090907", 
             "t4jds"."c4jdg_line_price__sum__145489358291", 
             "t4jds"."c4jdh_month_begin_date__min__838817", 
             "t4jds"."c4jdi_customer_state__max__67562250", 
             "t4jds"."c4jdo_mode_product_family" , 
             "t4jds"."c4jdp_product_subcategory__unique_v" 
        FROM   ( 
                     SELECT      "t4jdq"."c4jdd_store_phone_number_8958366756",
                             "t4jdq"."c4jde_line_cost__singleton__3098294",
                             "t4jdq"."c4jdf_yyyymmdd__mean__3928221090907",
                             "t4jdq"."c4jdg_line_price__sum__145489358291",
                             "t4jdq"."c4jdh_month_begin_date__min__838817",
                             "t4jdq"."c4jdi_customer_state__max__67562250",
                             "t4jdr"."c4jdo_mode_product_family" , 
                             "t4jdq"."c4jdp_product_subcategory__unique_v"
                     FROM      ( 
                                  SELECT   "t4jdc"."c4jcu_store_phone_number" AS "c4jdd_store_phone_number_8958366756",
                                       CASE 
                                          WHEN ( 
                                                    Count("t4jdc"."c4jd7_line_cost") = Count(*)
                                               AND    Min("t4jdc"."c4jd7_line_cost") = Max("t4jdc"."c4jd7_line_cost")) THEN Min("t4jdc"."c4jd7_line_cost")
                                       END AS "c4jde_line_cost__singleton__3098294",
                                       (Avg("c4jdb_m_yyyymmdd_yyyymm") /
                                       CASE 
                                          WHEN Avg("t4jdc"."c4jd3_yyyymm") = 0.0 THEN NULL
                                          ELSE Avg("t4jdc"."c4jd3_yyyymm")
                                       END)                        AS "c4jdf_yyyymmdd__mean__3928221090907",
                                       Sum("t4jdc"."c4jd6_line_price")           AS "c4jdg_line_price__sum__145489358291",
                                       Min("t4jdc"."c4jd2_month_begin_date")         AS "c4jdh_month_begin_date__min__838817",
                                       Max("t4jdc"."c4jcg_customer_state")         AS "c4jdi_customer_state__max__67562250",
                                       Count(DISTINCT "t4jdc"."c4jcm_product_subcategory") AS "c4jdp_product_subcategory__unique_v"
                                  FROM   ( 
                                          SELECT "t4jcc"."Customer_Gender"         AS "c4jcd_customer_gender" ,
                                             "t4jcc"."Customer_Region"         AS "c4jce_customer_region" ,
                                             "t4jcc"."Customer_Country"        AS "c4jcf_customer_country" ,
                                             "t4jcc"."Customer_State"        AS "c4jcg_customer_state" ,
                                             "t4jcc"."Customer_City"         AS "c4jch_customer_city" ,
                                             "t4jcc"."Customer_Date_Of_Birth"    AS "c4jci_customer_date_of_birth",
                                             "t4jcc"."Customer_Id"           AS "c4jcj_customer_id" ,
                                             "t4jcc"."Product_Family"        AS "c4jck_product_family" ,
                                             "t4jcc"."Product_Category"        AS "c4jcl_product_category" ,
                                             "t4jcc"."Product_Subcategory"       AS "c4jcm_product_subcategory" ,
                                             "t4jcc"."Product_Name"          AS "c4jcn_product_name" ,
                                             "t4jcc"."Product_Id"          AS "c4jco_product_id" ,
                                             "t4jcc"."Store_Id"            AS "c4jcp_store_id" ,
                                             "t4jcc"."Store_Longitude"         AS "c4jcq_store_longitude" ,
                                             "t4jcc"."Store_Latitude"        AS "c4jcr_store_latitude" ,
                                             "t4jcc"."Store_Name"          AS "c4jcs_store_name" ,
                                             "t4jcc"."Store_Manager"         AS "c4jct_store_manager" ,
                                             "t4jcc"."Store_Phone_Number"      AS "c4jcu_store_phone_number" ,
                                             "t4jcc"."Store_Region"          AS "c4jcv_store_region" ,
                                             "t4jcc"."Store_Country"         AS "c4jcw_store_country" ,
                                             "t4jcc"."Store_State"           AS "c4jcx_store_state" ,
                                             "t4jcc"."Store_City"          AS "c4jcy_store_city" ,
                                             "t4jcc"."Order_Date"          AS "c4jcz_order_date" ,
                                             "t4jcc"."Year_Begin_Date"         AS "c4jd0_year_begin_date" ,
                                             "t4jcc"."Quarter_Begin_Date"      AS "c4jd1_quarter_begin_date" ,
                                             "t4jcc"."Month_Begin_Date"        AS "c4jd2_month_begin_date" ,
                                             "t4jcc"."Yyyymm"            AS "c4jd3_yyyymm" ,
                                             "t4jcc"."Yyyymmdd"            AS "c4jd4_yyyymmdd" ,
                                             "t4jcc"."Ddmonyyyy"           AS "c4jd5_ddmonyyyy" ,
                                             "t4jcc"."Line_Price"          AS "c4jd6_line_price" ,
                                             "t4jcc"."Line_Cost"           AS "c4jd7_line_cost" ,
                                             "t4jcc"."Line_Margin"           AS "c4jd8_line_margin" ,
                                             "t4jcc"."Line_Margin_Percent"       AS "c4jd9_line_margin_percent" ,
                                             "t4jcc"."_visokio_row_id_"        AS "c4jda__visokio_row_id_" ,
                                             ("t4jcc"."Yyyymm" * "t4jcc"."Yyyymmdd") AS "c4jdb_m_yyyymmdd_yyyymm"
                                          FROM   ( 
                                                SELECT *
                                                FROM   "lineitem_denormalized_first1k_sanitised" AS "t4jcb") AS "t4jcc") AS "t4jdc"
                                  GROUP BY "t4jdc"."c4jcu_store_phone_number") AS "t4jdq"
                     LEFT OUTER JOIN 
                             ( 
                                SELECT "t4jdn"."c4jdj_modeg_c4jcu_store_phone_numbe",
                                     "t4jdn"."c4jdl_modec_product_family" AS "c4jdo_mode_product_family"
                                FROM   ( 
                                        SELECT   "t4jdc"."c4jcu_store_phone_number"                                                 AS "c4jdj_modeg_c4jcu_store_phone_numbe",
                                             "t4jdc"."c4jck_product_family"                                                   AS "c4jdl_modec_product_family" ,
                                             Count(*)                                                             AS "c4jdk_mode_count" ,
                                             Row_number() OVER ( partition BY "t4jdc"."c4jcu_store_phone_number" ORDER BY Count(*) DESC, "t4jdc"."c4jck_product_family" DESC) AS "c4jdm_mode_rank"
                                        FROM   ( 
                                                SELECT "t4jcc"."Customer_Gender"         AS "c4jcd_customer_gender" ,
                                                     "t4jcc"."Customer_Region"         AS "c4jce_customer_region" ,
                                                     "t4jcc"."Customer_Country"        AS "c4jcf_customer_country" ,
                                                     "t4jcc"."Customer_State"        AS "c4jcg_customer_state" ,
                                                     "t4jcc"."Customer_City"         AS "c4jch_customer_city" ,
                                                     "t4jcc"."Customer_Date_Of_Birth"    AS "c4jci_customer_date_of_birth",
                                                     "t4jcc"."Customer_Id"           AS "c4jcj_customer_id" ,
                                                     "t4jcc"."Product_Family"        AS "c4jck_product_family" ,
                                                     "t4jcc"."Product_Category"        AS "c4jcl_product_category" ,
                                                     "t4jcc"."Product_Subcategory"       AS "c4jcm_product_subcategory" ,
                                                     "t4jcc"."Product_Name"          AS "c4jcn_product_name" ,
                                                     "t4jcc"."Product_Id"          AS "c4jco_product_id" ,
                                                     "t4jcc"."Store_Id"            AS "c4jcp_store_id" ,
                                                     "t4jcc"."Store_Longitude"         AS "c4jcq_store_longitude" ,
                                                     "t4jcc"."Store_Latitude"        AS "c4jcr_store_latitude" ,
                                                     "t4jcc"."Store_Name"          AS "c4jcs_store_name" ,
                                                     "t4jcc"."Store_Manager"         AS "c4jct_store_manager" ,
                                                     "t4jcc"."Store_Phone_Number"      AS "c4jcu_store_phone_number" ,
                                                     "t4jcc"."Store_Region"          AS "c4jcv_store_region" ,
                                                     "t4jcc"."Store_Country"         AS "c4jcw_store_country" ,
                                                     "t4jcc"."Store_State"           AS "c4jcx_store_state" ,
                                                     "t4jcc"."Store_City"          AS "c4jcy_store_city" ,
                                                     "t4jcc"."Order_Date"          AS "c4jcz_order_date" ,
                                                     "t4jcc"."Year_Begin_Date"         AS "c4jd0_year_begin_date" ,
                                                     "t4jcc"."Quarter_Begin_Date"      AS "c4jd1_quarter_begin_date" ,
                                                     "t4jcc"."Month_Begin_Date"        AS "c4jd2_month_begin_date" ,
                                                     "t4jcc"."Yyyymm"            AS "c4jd3_yyyymm" ,
                                                     "t4jcc"."Yyyymmdd"            AS "c4jd4_yyyymmdd" ,
                                                     "t4jcc"."Ddmonyyyy"           AS "c4jd5_ddmonyyyy" ,
                                                     "t4jcc"."Line_Price"          AS "c4jd6_line_price" ,
                                                     "t4jcc"."Line_Cost"           AS "c4jd7_line_cost" ,
                                                     "t4jcc"."Line_Margin"           AS "c4jd8_line_margin" ,
                                                     "t4jcc"."Line_Margin_Percent"       AS "c4jd9_line_margin_percent" ,
                                                     "t4jcc"."_visokio_row_id_"        AS "c4jda__visokio_row_id_" ,
                                                     ("t4jcc"."Yyyymm" * "t4jcc"."Yyyymmdd") AS "c4jdb_m_yyyymmdd_yyyymm"
                                                FROM   (
                                                        SELECT *
                                                        FROM   "lineitem_denormalized_first1k_sanitised" AS "t4jcb") AS "t4jcc") AS "t4jdc"
                                        WHERE  "t4jdc"."c4jck_product_family" IS NOT NULL
                                        GROUP BY "t4jdc"."c4jcu_store_phone_number",
                                             "t4jdc"."c4jck_product_family") AS "t4jdn"
                                WHERE  ( 
                                        "t4jdn"."c4jdm_mode_rank" = 1
                                     AND  "t4jdn"."c4jdk_mode_count" > 1)) AS "t4jdr"
                     ON        ( 
                                     "t4jdq"."c4jdd_store_phone_number_8958366756" = "t4jdr"."c4jdj_modeg_c4jcu_store_phone_numbe"
                             OR        ( 
                                             "t4jdq"."c4jdd_store_phone_number_8958366756" IS NULL
                                     AND       "t4jdr"."c4jdj_modeg_c4jcu_store_phone_numbe" IS NULL))) AS "t4jds"
        ORDER BY "c4jdd_store_phone_number_8958366756" ASC limit 1000) AS "t4jdu" limit 125000;


rollback;
