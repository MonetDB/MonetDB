from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # TODO match on the output
    tc.execute('''
start transaction;
create table dbgen_version
(
 dv_version varchar(16),
 dv_create_date date,
 dv_create_time time,
 dv_cmdline_args varchar(200)
);
create table customer_address
(
 ca_address_sk integer not null,
 ca_address_id char(16) not null,
 ca_street_number char(10),
 ca_street_name varchar(60),
 ca_street_type char(15),
 ca_suite_number char(10),
 ca_city varchar(60),
 ca_county varchar(30),
 ca_state char(2),
 ca_zip char(10),
 ca_country varchar(20),
 ca_gmt_offset decimal(5,2),
 ca_location_type char(20),
 primary key (ca_address_sk)
);
create table customer_demographics
(
 cd_demo_sk integer not null,
 cd_gender char(1),
 cd_marital_status char(1),
 cd_education_status char(20),
 cd_purchase_estimate integer,
 cd_credit_rating char(10),
 cd_dep_count integer,
 cd_dep_employed_count integer,
 cd_dep_college_count integer,
 primary key (cd_demo_sk)
);
create table date_dim
(
 d_date_sk integer not null,
 d_date_id char(16) not null,
 d_date date,
 d_month_seq integer,
 d_week_seq integer,
 d_quarter_seq integer,
 d_year integer,
 d_dow integer,
 d_moy integer,
 d_dom integer,
 d_qoy integer,
 d_fy_year integer,
 d_fy_quarter_seq integer,
 d_fy_week_seq integer,
 d_day_name char(9),
 d_quarter_name char(6),
 d_holiday char(1),
 d_weekend char(1),
 d_following_holiday char(1),
 d_first_dom integer,
 d_last_dom integer,
 d_same_day_ly integer,
 d_same_day_lq integer,
 d_current_day char(1),
 d_current_week char(1),
 d_current_month char(1),
 d_current_quarter char(1),
 d_current_year char(1),
 primary key (d_date_sk)
);
create table warehouse
(
 w_warehouse_sk integer not null,
 w_warehouse_id char(16) not null,
 w_warehouse_name varchar(20),
 w_warehouse_sq_ft integer,
 w_street_number char(10),
 w_street_name varchar(60),
 w_street_type char(15),
 w_suite_number char(10),
 w_city varchar(60),
 w_county varchar(30),
 w_state char(2),
 w_zip char(10),
 w_country varchar(20),
 w_gmt_offset decimal(5,2),
 primary key (w_warehouse_sk)
);
create table ship_mode
(
 sm_ship_mode_sk integer not null,
 sm_ship_mode_id char(16) not null,
 sm_type char(30),
 sm_code char(10),
 sm_carrier char(20),
 sm_contract char(20),
 primary key (sm_ship_mode_sk)
);
create table time_dim
(
 t_time_sk integer not null,
 t_time_id char(16) not null,
 t_time integer,
 t_hour integer,
 t_minute integer,
 t_second integer,
 t_am_pm char(2),
 t_shift char(20),
 t_sub_shift char(20),
 t_meal_time char(20),
 primary key (t_time_sk)
);
create table reason
(
 r_reason_sk integer not null,
 r_reason_id char(16) not null,
 r_reason_desc char(100),
 primary key (r_reason_sk)
);
create table income_band
(
 ib_income_band_sk integer not null,
 ib_lower_bound integer,
 ib_upper_bound integer,
 primary key (ib_income_band_sk)
);
create table item
(
 i_item_sk integer not null,
 i_item_id char(16) not null,
 i_rec_start_date date,
 i_rec_end_date date,
 i_item_desc varchar(200),
 i_current_price decimal(7,2),
 i_wholesale_cost decimal(7,2),
 i_brand_id integer,
 i_brand char(50),
 i_class_id integer,
 i_class char(50),
 i_category_id integer,
 i_category char(50),
 i_manufact_id integer,
 i_manufact char(50),
 i_size char(20),
 i_formulation char(20),
 i_color char(20),
 i_units char(10),
 i_container char(10),
 i_manager_id integer,
 i_product_name char(50),
 primary key (i_item_sk)
);
create table store
(
 s_store_sk integer not null,
 s_store_id char(16) not null,
 s_rec_start_date date,
 s_rec_end_date date,
 s_closed_date_sk integer,
 s_store_name varchar(50),
 s_number_employees integer,
 s_floor_space integer,
 s_hours char(20),
 s_manager varchar(40),
 s_market_id integer,
 s_geography_class varchar(100),
 s_market_desc varchar(100),
 s_market_manager varchar(40),
 s_division_id integer,
 s_division_name varchar(50),
 s_company_id integer,
 s_company_name varchar(50),
 s_street_number varchar(10),
 s_street_name varchar(60),
 s_street_type char(15),
 s_suite_number char(10),
 s_city varchar(60),
 s_county varchar(30),
 s_state char(2),
 s_zip char(10),
 s_country varchar(20),
 s_gmt_offset decimal(5,2),
 s_tax_precentage decimal(5,2),
 primary key (s_store_sk)
);
create table call_center
(
 cc_call_center_sk integer not null,
 cc_call_center_id char(16) not null,
 cc_rec_start_date date,
 cc_rec_end_date date,
 cc_closed_date_sk integer,
 cc_open_date_sk integer,
 cc_name varchar(50),
 cc_class varchar(50),
 cc_employees integer,
 cc_sq_ft integer,
 cc_hours char(20),
 cc_manager varchar(40),
 cc_mkt_id integer,
 cc_mkt_class char(50),
 cc_mkt_desc varchar(100),
 cc_market_manager varchar(40),
 cc_division integer,
 cc_division_name varchar(50),
 cc_company integer,
 cc_company_name char(50),
 cc_street_number char(10),
 cc_street_name varchar(60),
 cc_street_type char(15),
 cc_suite_number char(10),
 cc_city varchar(60),
 cc_county varchar(30),
 cc_state char(2),
 cc_zip char(10),
 cc_country varchar(20),
 cc_gmt_offset decimal(5,2),
 cc_tax_percentage decimal(5,2),
 primary key (cc_call_center_sk)
);
create table customer
(
 c_customer_sk integer not null,
 c_customer_id char(16) not null,
 c_current_cdemo_sk integer,
 c_current_hdemo_sk integer,
 c_current_addr_sk integer,
 c_first_shipto_date_sk integer,
 c_first_sales_date_sk integer,
 c_salutation char(10),
 c_first_name char(20),
 c_last_name char(30),
 c_preferred_cust_flag char(1),
 c_birth_day integer,
 c_birth_month integer,
 c_birth_year integer,
 c_birth_country varchar(20),
 c_login char(13),
 c_email_address char(50),
 c_last_review_date_sk integer,
 primary key (c_customer_sk)
);
create table web_site
(
 web_site_sk integer not null,
 web_site_id char(16) not null,
 web_rec_start_date date,
 web_rec_end_date date,
 web_name varchar(50),
 web_open_date_sk integer,
 web_close_date_sk integer,
 web_class varchar(50),
 web_manager varchar(40),
 web_mkt_id integer,
 web_mkt_class varchar(50),
 web_mkt_desc varchar(100),
 web_market_manager varchar(40),
 web_company_id integer,
 web_company_name char(50),
 web_street_number char(10),
 web_street_name varchar(60),
 web_street_type char(15),
 web_suite_number char(10),
 web_city varchar(60),
 web_county varchar(30),
 web_state char(2),
 web_zip char(10),
 web_country varchar(20),
 web_gmt_offset decimal(5,2),
 web_tax_percentage decimal(5,2),
 primary key (web_site_sk)
);
create table store_returns
(
 sr_returned_date_sk integer,
 sr_return_time_sk integer,
 sr_item_sk integer not null,
 sr_customer_sk integer,
 sr_cdemo_sk integer,
 sr_hdemo_sk integer,
 sr_addr_sk integer,
 sr_store_sk integer,
 sr_reason_sk integer,
 sr_ticket_number integer not null,
 sr_return_quantity integer,
 sr_return_amt decimal(7,2),
 sr_return_tax decimal(7,2),
 sr_return_amt_inc_tax decimal(7,2),
 sr_fee decimal(7,2),
 sr_return_ship_cost decimal(7,2),
 sr_refunded_cash decimal(7,2),
 sr_reversed_charge decimal(7,2),
 sr_store_credit decimal(7,2),
 sr_net_loss decimal(7,2),
 primary key (sr_item_sk, sr_ticket_number)
);
create table household_demographics
(
 hd_demo_sk integer not null,
 hd_income_band_sk integer,
 hd_buy_potential char(15),
 hd_dep_count integer,
 hd_vehicle_count integer,
 primary key (hd_demo_sk)
);
create table web_page
(
 wp_web_page_sk integer not null,
 wp_web_page_id char(16) not null,
 wp_rec_start_date date,
 wp_rec_end_date date,
 wp_creation_date_sk integer,
 wp_access_date_sk integer,
 wp_autogen_flag char(1),
 wp_customer_sk integer,
 wp_url varchar(100),
 wp_type char(50),
 wp_char_count integer,
 wp_link_count integer,
 wp_image_count integer,
 wp_max_ad_count integer,
 primary key (wp_web_page_sk)
);
create table promotion
(
 p_promo_sk integer not null,
 p_promo_id char(16) not null,
 p_start_date_sk integer,
 p_end_date_sk integer,
 p_item_sk integer,
 p_cost decimal(15,2),
 p_response_target integer,
 p_promo_name char(50),
 p_channel_dmail char(1),
 p_channel_email char(1),
 p_channel_catalog char(1),
 p_channel_tv char(1),
 p_channel_radio char(1),
 p_channel_press char(1),
 p_channel_event char(1),
 p_channel_demo char(1),
 p_channel_details varchar(100),
 p_purpose char(15),
 p_discount_active char(1),
 primary key (p_promo_sk)
);
create table catalog_page
(
 cp_catalog_page_sk integer not null,
 cp_catalog_page_id char(16) not null,
 cp_start_date_sk integer,
 cp_end_date_sk integer,
 cp_department varchar(50),
 cp_catalog_number integer,
 cp_catalog_page_number integer,
 cp_description varchar(100),
 cp_type varchar(100),
 primary key (cp_catalog_page_sk)
);
create table inventory
(
 inv_date_sk integer not null,
 inv_item_sk integer not null,
 inv_warehouse_sk integer not null,
 inv_quantity_on_hand integer,
 primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);
create table catalog_returns
(
 cr_returned_date_sk integer,
 cr_returned_time_sk integer,
 cr_item_sk integer not null,
 cr_refunded_customer_sk integer,
 cr_refunded_cdemo_sk integer,
 cr_refunded_hdemo_sk integer,
 cr_refunded_addr_sk integer,
 cr_returning_customer_sk integer,
 cr_returning_cdemo_sk integer,
 cr_returning_hdemo_sk integer,
 cr_returning_addr_sk integer,
 cr_call_center_sk integer,
 cr_catalog_page_sk integer,
 cr_ship_mode_sk integer,
 cr_warehouse_sk integer,
 cr_reason_sk integer,
 cr_order_number integer not null,
 cr_return_quantity integer,
 cr_return_amount decimal(7,2),
 cr_return_tax decimal(7,2),
 cr_return_amt_inc_tax decimal(7,2),
 cr_fee decimal(7,2),
 cr_return_ship_cost decimal(7,2),
 cr_refunded_cash decimal(7,2),
 cr_reversed_charge decimal(7,2),
 cr_store_credit decimal(7,2),
 cr_net_loss decimal(7,2),
 primary key (cr_item_sk, cr_order_number)
);
create table web_returns
(
 wr_returned_date_sk integer,
 wr_returned_time_sk integer,
 wr_item_sk integer not null,
 wr_refunded_customer_sk integer,
 wr_refunded_cdemo_sk integer,
 wr_refunded_hdemo_sk integer,
 wr_refunded_addr_sk integer,
 wr_returning_customer_sk integer,
 wr_returning_cdemo_sk integer,
 wr_returning_hdemo_sk integer,
 wr_returning_addr_sk integer,
 wr_web_page_sk integer,
 wr_reason_sk integer,
 wr_order_number integer not null,
 wr_return_quantity integer,
 wr_return_amt decimal(7,2),
 wr_return_tax decimal(7,2),
 wr_return_amt_inc_tax decimal(7,2),
 wr_fee decimal(7,2),
 wr_return_ship_cost decimal(7,2),
 wr_refunded_cash decimal(7,2),
 wr_reversed_charge decimal(7,2),
 wr_account_credit decimal(7,2),
 wr_net_loss decimal(7,2),
 primary key (wr_item_sk, wr_order_number)
);
create table web_sales
(
 ws_sold_date_sk integer,
 ws_sold_time_sk integer,
 ws_ship_date_sk integer,
 ws_item_sk integer not null,
 ws_bill_customer_sk integer,
 ws_bill_cdemo_sk integer,
 ws_bill_hdemo_sk integer,
 ws_bill_addr_sk integer,
 ws_ship_customer_sk integer,
 ws_ship_cdemo_sk integer,
 ws_ship_hdemo_sk integer,
 ws_ship_addr_sk integer,
 ws_web_page_sk integer,
 ws_web_site_sk integer,
 ws_ship_mode_sk integer,
 ws_warehouse_sk integer,
 ws_promo_sk integer,
 ws_order_number integer not null,
 ws_quantity integer,
 ws_wholesale_cost decimal(7,2),
 ws_list_price decimal(7,2),
 ws_sales_price decimal(7,2),
 ws_ext_discount_amt decimal(7,2),
 ws_ext_sales_price decimal(7,2),
 ws_ext_wholesale_cost decimal(7,2),
 ws_ext_list_price decimal(7,2),
 ws_ext_tax decimal(7,2),
 ws_coupon_amt decimal(7,2),
 ws_ext_ship_cost decimal(7,2),
 ws_net_paid decimal(7,2),
 ws_net_paid_inc_tax decimal(7,2),
 ws_net_paid_inc_ship decimal(7,2),
 ws_net_paid_inc_ship_tax decimal(7,2),
 ws_net_profit decimal(7,2),
 primary key (ws_item_sk, ws_order_number)
);
create table catalog_sales
(
 cs_sold_date_sk integer,
 cs_sold_time_sk integer,
 cs_ship_date_sk integer,
 cs_bill_customer_sk integer,
 cs_bill_cdemo_sk integer,
 cs_bill_hdemo_sk integer,
 cs_bill_addr_sk integer,
 cs_ship_customer_sk integer,
 cs_ship_cdemo_sk integer,
 cs_ship_hdemo_sk integer,
 cs_ship_addr_sk integer,
 cs_call_center_sk integer,
 cs_catalog_page_sk integer,
 cs_ship_mode_sk integer,
 cs_warehouse_sk integer,
 cs_item_sk integer not null,
 cs_promo_sk integer,
 cs_order_number integer not null,
 cs_quantity integer,
 cs_wholesale_cost decimal(7,2),
 cs_list_price decimal(7,2),
 cs_sales_price decimal(7,2),
 cs_ext_discount_amt decimal(7,2),
 cs_ext_sales_price decimal(7,2),
 cs_ext_wholesale_cost decimal(7,2),
 cs_ext_list_price decimal(7,2),
 cs_ext_tax decimal(7,2),
 cs_coupon_amt decimal(7,2),
 cs_ext_ship_cost decimal(7,2),
 cs_net_paid decimal(7,2),
 cs_net_paid_inc_tax decimal(7,2),
 cs_net_paid_inc_ship decimal(7,2),
 cs_net_paid_inc_ship_tax decimal(7,2),
 cs_net_profit decimal(7,2),
 primary key (cs_item_sk, cs_order_number)
);
create table store_sales
(
 ss_sold_date_sk integer,
 ss_sold_time_sk integer,
 ss_item_sk integer not null,
 ss_customer_sk integer,
 ss_cdemo_sk integer,
 ss_hdemo_sk integer,
 ss_addr_sk integer,
 ss_store_sk integer,
 ss_promo_sk integer,
 ss_ticket_number integer not null,
 ss_quantity integer,
 ss_wholesale_cost decimal(7,2),
 ss_list_price decimal(7,2),
 ss_sales_price decimal(7,2),
 ss_ext_discount_amt decimal(7,2),
 ss_ext_sales_price decimal(7,2),
 ss_ext_wholesale_cost decimal(7,2),
 ss_ext_list_price decimal(7,2),
 ss_ext_tax decimal(7,2),
 ss_coupon_amt decimal(7,2),
 ss_net_paid decimal(7,2),
 ss_net_paid_inc_tax decimal(7,2),
 ss_net_profit decimal(7,2),
 primary key (ss_item_sk, ss_ticket_number)
);
alter table call_center add constraint cc_d1 foreign key (cc_closed_date_sk) references date_dim (d_date_sk);
alter table call_center add constraint cc_d2 foreign key (cc_open_date_sk) references date_dim (d_date_sk);
alter table catalog_page add constraint cp_d1 foreign key (cp_end_date_sk) references date_dim (d_date_sk);
alter table catalog_page add constraint cp_d2 foreign key (cp_start_date_sk) references date_dim (d_date_sk);
alter table catalog_returns add constraint cr_cc foreign key (cr_call_center_sk) references call_center (cc_call_center_sk);
alter table catalog_returns add constraint cr_cp foreign key (cr_catalog_page_sk) references catalog_page (cp_catalog_page_sk);
alter table catalog_returns add constraint cr_i foreign key (cr_item_sk) references item (i_item_sk);
alter table catalog_returns add constraint cr_r foreign key (cr_reason_sk) references reason (r_reason_sk);
alter table catalog_returns add constraint cr_a1 foreign key (cr_refunded_addr_sk) references customer_address (ca_address_sk);
alter table catalog_returns add constraint cr_cd1 foreign key (cr_refunded_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table catalog_returns add constraint cr_c1 foreign key (cr_refunded_customer_sk) references customer (c_customer_sk);
alter table catalog_returns add constraint cr_hd1 foreign key (cr_refunded_hdemo_sk) references household_demographics (hd_demo_sk);
alter table catalog_returns add constraint cr_d1 foreign key (cr_returned_date_sk) references date_dim (d_date_sk);
alter table catalog_returns add constraint cr_t foreign key (cr_returned_time_sk) references time_dim (t_time_sk);
alter table catalog_returns add constraint cr_a2 foreign key (cr_returning_addr_sk) references customer_address (ca_address_sk);
alter table catalog_returns add constraint cr_cd2 foreign key (cr_returning_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table catalog_returns add constraint cr_c2 foreign key (cr_returning_customer_sk) references customer (c_customer_sk);
alter table catalog_returns add constraint cr_hd2 foreign key (cr_returning_hdemo_sk) references household_demographics (hd_demo_sk);
alter table catalog_returns add constraint cr_sm foreign key (cr_ship_mode_sk) references ship_mode (sm_ship_mode_sk);
alter table catalog_returns add constraint cr_w2 foreign key (cr_warehouse_sk) references warehouse (w_warehouse_sk);
alter table catalog_sales add constraint cs_b_a foreign key (cs_bill_addr_sk) references customer_address (ca_address_sk);
alter table catalog_sales add constraint cs_b_cd foreign key (cs_bill_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table catalog_sales add constraint cs_b_c foreign key (cs_bill_customer_sk) references customer (c_customer_sk);
alter table catalog_sales add constraint cs_b_hd foreign key (cs_bill_hdemo_sk) references household_demographics (hd_demo_sk);
alter table catalog_sales add constraint cs_cc foreign key (cs_call_center_sk) references call_center (cc_call_center_sk);
alter table catalog_sales add constraint cs_cp foreign key (cs_catalog_page_sk) references catalog_page (cp_catalog_page_sk);
alter table catalog_sales add constraint cs_i foreign key (cs_item_sk) references item (i_item_sk);
alter table catalog_sales add constraint cs_p foreign key (cs_promo_sk) references promotion (p_promo_sk);
alter table catalog_sales add constraint cs_s_a foreign key (cs_ship_addr_sk) references customer_address (ca_address_sk);
alter table catalog_sales add constraint cs_s_cd foreign key (cs_ship_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table catalog_sales add constraint cs_s_c foreign key (cs_ship_customer_sk) references customer (c_customer_sk);
alter table catalog_sales add constraint cs_d1 foreign key (cs_ship_date_sk) references date_dim (d_date_sk);
alter table catalog_sales add constraint cs_s_hd foreign key (cs_ship_hdemo_sk) references household_demographics (hd_demo_sk);
alter table catalog_sales add constraint cs_sm foreign key (cs_ship_mode_sk) references ship_mode (sm_ship_mode_sk);
alter table catalog_sales add constraint cs_d2 foreign key (cs_sold_date_sk) references date_dim (d_date_sk);
alter table catalog_sales add constraint cs_t foreign key (cs_sold_time_sk) references time_dim (t_time_sk);
alter table catalog_sales add constraint cs_w foreign key (cs_warehouse_sk) references warehouse (w_warehouse_sk);
alter table customer add constraint c_a foreign key (c_current_addr_sk) references customer_address (ca_address_sk);
alter table customer add constraint c_cd foreign key (c_current_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table customer add constraint c_hd foreign key (c_current_hdemo_sk) references household_demographics (hd_demo_sk);
alter table customer add constraint c_fsd foreign key (c_first_sales_date_sk) references date_dim (d_date_sk);
alter table customer add constraint c_fsd2 foreign key (c_first_shipto_date_sk) references date_dim (d_date_sk);
alter table household_demographics add constraint hd_ib foreign key (hd_income_band_sk) references income_band (ib_income_band_sk);
alter table inventory add constraint inv_d foreign key (inv_date_sk) references date_dim (d_date_sk);
alter table inventory add constraint inv_i foreign key (inv_item_sk) references item (i_item_sk);
alter table inventory add constraint inv_w foreign key (inv_warehouse_sk) references warehouse (w_warehouse_sk);
alter table promotion add constraint p_end_date foreign key (p_end_date_sk) references date_dim (d_date_sk);
alter table promotion add constraint p_i foreign key (p_item_sk) references item (i_item_sk);
alter table promotion add constraint p_start_date foreign key (p_start_date_sk) references date_dim (d_date_sk);
alter table store add constraint s_close_date foreign key (s_closed_date_sk) references date_dim (d_date_sk);
alter table store_returns add constraint sr_a foreign key (sr_addr_sk) references customer_address (ca_address_sk);
alter table store_returns add constraint sr_cd foreign key (sr_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table store_returns add constraint sr_c foreign key (sr_customer_sk) references customer (c_customer_sk);
alter table store_returns add constraint sr_hd foreign key (sr_hdemo_sk) references household_demographics (hd_demo_sk);
alter table store_returns add constraint sr_i foreign key (sr_item_sk) references item (i_item_sk);
alter table store_returns add constraint sr_r foreign key (sr_reason_sk) references reason (r_reason_sk);
alter table store_returns add constraint sr_ret_d foreign key (sr_returned_date_sk) references date_dim (d_date_sk);
alter table store_returns add constraint sr_t foreign key (sr_return_time_sk) references time_dim (t_time_sk);
alter table store_returns add constraint sr_s foreign key (sr_store_sk) references store (s_store_sk);
alter table store_sales add constraint ss_a foreign key (ss_addr_sk) references customer_address (ca_address_sk);
alter table store_sales add constraint ss_cd foreign key (ss_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table store_sales add constraint ss_c foreign key (ss_customer_sk) references customer (c_customer_sk);
alter table store_sales add constraint ss_hd foreign key (ss_hdemo_sk) references household_demographics (hd_demo_sk);
alter table store_sales add constraint ss_i foreign key (ss_item_sk) references item (i_item_sk);
alter table store_sales add constraint ss_p foreign key (ss_promo_sk) references promotion (p_promo_sk);
alter table store_sales add constraint ss_d foreign key (ss_sold_date_sk) references date_dim (d_date_sk);
alter table store_sales add constraint ss_t foreign key (ss_sold_time_sk) references time_dim (t_time_sk);
alter table store_sales add constraint ss_s foreign key (ss_store_sk) references store (s_store_sk);
alter table web_page add constraint wp_ad foreign key (wp_access_date_sk) references date_dim (d_date_sk);
alter table web_page add constraint wp_cd foreign key (wp_creation_date_sk) references date_dim (d_date_sk);
alter table web_returns add constraint wr_i foreign key (wr_item_sk) references item (i_item_sk);
alter table web_returns add constraint wr_r foreign key (wr_reason_sk) references reason (r_reason_sk);
alter table web_returns add constraint wr_ref_a foreign key (wr_refunded_addr_sk) references customer_address (ca_address_sk);
alter table web_returns add constraint wr_ref_cd foreign key (wr_refunded_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table web_returns add constraint wr_ref_c foreign key (wr_refunded_customer_sk) references customer (c_customer_sk);
alter table web_returns add constraint wr_ref_hd foreign key (wr_refunded_hdemo_sk) references household_demographics (hd_demo_sk);
alter table web_returns add constraint wr_ret_d foreign key (wr_returned_date_sk) references date_dim (d_date_sk);
alter table web_returns add constraint wr_ret_t foreign key (wr_returned_time_sk) references time_dim (t_time_sk);
alter table web_returns add constraint wr_ret_a foreign key (wr_returning_addr_sk) references customer_address (ca_address_sk);
alter table web_returns add constraint wr_ret_cd foreign key (wr_returning_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table web_returns add constraint wr_ret_c foreign key (wr_returning_customer_sk) references customer (c_customer_sk);
alter table web_returns add constraint wr_ret_hd foreign key (wr_returning_hdemo_sk) references household_demographics (hd_demo_sk);
alter table web_returns add constraint wr_wp foreign key (wr_web_page_sk) references web_page (wp_web_page_sk);
alter table web_sales add constraint ws_b_a foreign key (ws_bill_addr_sk) references customer_address (ca_address_sk);
alter table web_sales add constraint ws_b_cd foreign key (ws_bill_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table web_sales add constraint ws_b_c foreign key (ws_bill_customer_sk) references customer (c_customer_sk);
alter table web_sales add constraint ws_b_hd foreign key (ws_bill_hdemo_sk) references household_demographics (hd_demo_sk);
alter table web_sales add constraint ws_i foreign key (ws_item_sk) references item (i_item_sk);
alter table web_sales add constraint ws_p foreign key (ws_promo_sk) references promotion (p_promo_sk);
alter table web_sales add constraint ws_s_a foreign key (ws_ship_addr_sk) references customer_address (ca_address_sk);
alter table web_sales add constraint ws_s_cd foreign key (ws_ship_cdemo_sk) references customer_demographics (cd_demo_sk);
alter table web_sales add constraint ws_s_c foreign key (ws_ship_customer_sk) references customer (c_customer_sk);
alter table web_sales add constraint ws_s_d foreign key (ws_ship_date_sk) references date_dim (d_date_sk);
alter table web_sales add constraint ws_s_hd foreign key (ws_ship_hdemo_sk) references household_demographics (hd_demo_sk);
alter table web_sales add constraint ws_sm foreign key (ws_ship_mode_sk) references ship_mode (sm_ship_mode_sk);
alter table web_sales add constraint ws_d2 foreign key (ws_sold_date_sk) references date_dim (d_date_sk);
alter table web_sales add constraint ws_t foreign key (ws_sold_time_sk) references time_dim (t_time_sk);
alter table web_sales add constraint ws_w2 foreign key (ws_warehouse_sk) references warehouse (w_warehouse_sk);
alter table web_sales add constraint ws_wp foreign key (ws_web_page_sk) references web_page (wp_web_page_sk);
alter table web_sales add constraint ws_ws foreign key (ws_web_site_sk) references web_site (web_site_sk);
alter table web_site add constraint web_d1 foreign key (web_close_date_sk) references date_dim (d_date_sk);
alter table web_site add constraint web_d2 foreign key (web_open_date_sk) references date_dim (d_date_sk);
commit;
            ''', client='mclient').assertSucceeded()
    tc.sqldump().assertMatchStableOut(fout='msqldump-mapi-cache.Bug-6777.stable.out')
    tc.execute('''
start transaction;
drop table if exists dbgen_version cascade;
drop table if exists customer_address cascade;
drop table if exists customer_demographics cascade;
drop table if exists date_dim cascade;
drop table if exists warehouse cascade;
drop table if exists ship_mode cascade;
drop table if exists time_dim cascade;
drop table if exists reason cascade;
drop table if exists income_band cascade;
drop table if exists item cascade;
drop table if exists store cascade;
drop table if exists call_center cascade;
drop table if exists customer cascade;
drop table if exists web_site cascade;
drop table if exists store_returns cascade;
drop table if exists household_demographics cascade;
drop table if exists web_page cascade;
drop table if exists promotion cascade;
drop table if exists catalog_page cascade;
drop table if exists inventory cascade;
drop table if exists catalog_returns cascade;
drop table if exists web_returns cascade;
drop table if exists web_sales cascade;
drop table if exists catalog_sales cascade;
drop table if exists store_sales cascade;
commit;
            ''').assertSucceeded()


#import sys
#
#try:
#    from MonetDBtesting import process
#except ImportError:
#    import process
#
#with process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
#    out, err = c.communicate('''
#start transaction;
#create table dbgen_version
#(
# dv_version varchar(16),
# dv_create_date date,
# dv_create_time time,
# dv_cmdline_args varchar(200)
#);
#create table customer_address
#(
# ca_address_sk integer not null,
# ca_address_id char(16) not null,
# ca_street_number char(10),
# ca_street_name varchar(60),
# ca_street_type char(15),
# ca_suite_number char(10),
# ca_city varchar(60),
# ca_county varchar(30),
# ca_state char(2),
# ca_zip char(10),
# ca_country varchar(20),
# ca_gmt_offset decimal(5,2),
# ca_location_type char(20),
# primary key (ca_address_sk)
#);
#create table customer_demographics
#(
# cd_demo_sk integer not null,
# cd_gender char(1),
# cd_marital_status char(1),
# cd_education_status char(20),
# cd_purchase_estimate integer,
# cd_credit_rating char(10),
# cd_dep_count integer,
# cd_dep_employed_count integer,
# cd_dep_college_count integer,
# primary key (cd_demo_sk)
#);
#create table date_dim
#(
# d_date_sk integer not null,
# d_date_id char(16) not null,
# d_date date,
# d_month_seq integer,
# d_week_seq integer,
# d_quarter_seq integer,
# d_year integer,
# d_dow integer,
# d_moy integer,
# d_dom integer,
# d_qoy integer,
# d_fy_year integer,
# d_fy_quarter_seq integer,
# d_fy_week_seq integer,
# d_day_name char(9),
# d_quarter_name char(6),
# d_holiday char(1),
# d_weekend char(1),
# d_following_holiday char(1),
# d_first_dom integer,
# d_last_dom integer,
# d_same_day_ly integer,
# d_same_day_lq integer,
# d_current_day char(1),
# d_current_week char(1),
# d_current_month char(1),
# d_current_quarter char(1),
# d_current_year char(1),
# primary key (d_date_sk)
#);
#create table warehouse
#(
# w_warehouse_sk integer not null,
# w_warehouse_id char(16) not null,
# w_warehouse_name varchar(20),
# w_warehouse_sq_ft integer,
# w_street_number char(10),
# w_street_name varchar(60),
# w_street_type char(15),
# w_suite_number char(10),
# w_city varchar(60),
# w_county varchar(30),
# w_state char(2),
# w_zip char(10),
# w_country varchar(20),
# w_gmt_offset decimal(5,2),
# primary key (w_warehouse_sk)
#);
#create table ship_mode
#(
# sm_ship_mode_sk integer not null,
# sm_ship_mode_id char(16) not null,
# sm_type char(30),
# sm_code char(10),
# sm_carrier char(20),
# sm_contract char(20),
# primary key (sm_ship_mode_sk)
#);
#create table time_dim
#(
# t_time_sk integer not null,
# t_time_id char(16) not null,
# t_time integer,
# t_hour integer,
# t_minute integer,
# t_second integer,
# t_am_pm char(2),
# t_shift char(20),
# t_sub_shift char(20),
# t_meal_time char(20),
# primary key (t_time_sk)
#);
#create table reason
#(
# r_reason_sk integer not null,
# r_reason_id char(16) not null,
# r_reason_desc char(100),
# primary key (r_reason_sk)
#);
#create table income_band
#(
# ib_income_band_sk integer not null,
# ib_lower_bound integer,
# ib_upper_bound integer,
# primary key (ib_income_band_sk)
#);
#create table item
#(
# i_item_sk integer not null,
# i_item_id char(16) not null,
# i_rec_start_date date,
# i_rec_end_date date,
# i_item_desc varchar(200),
# i_current_price decimal(7,2),
# i_wholesale_cost decimal(7,2),
# i_brand_id integer,
# i_brand char(50),
# i_class_id integer,
# i_class char(50),
# i_category_id integer,
# i_category char(50),
# i_manufact_id integer,
# i_manufact char(50),
# i_size char(20),
# i_formulation char(20),
# i_color char(20),
# i_units char(10),
# i_container char(10),
# i_manager_id integer,
# i_product_name char(50),
# primary key (i_item_sk)
#);
#create table store
#(
# s_store_sk integer not null,
# s_store_id char(16) not null,
# s_rec_start_date date,
# s_rec_end_date date,
# s_closed_date_sk integer,
# s_store_name varchar(50),
# s_number_employees integer,
# s_floor_space integer,
# s_hours char(20),
# s_manager varchar(40),
# s_market_id integer,
# s_geography_class varchar(100),
# s_market_desc varchar(100),
# s_market_manager varchar(40),
# s_division_id integer,
# s_division_name varchar(50),
# s_company_id integer,
# s_company_name varchar(50),
# s_street_number varchar(10),
# s_street_name varchar(60),
# s_street_type char(15),
# s_suite_number char(10),
# s_city varchar(60),
# s_county varchar(30),
# s_state char(2),
# s_zip char(10),
# s_country varchar(20),
# s_gmt_offset decimal(5,2),
# s_tax_precentage decimal(5,2),
# primary key (s_store_sk)
#);
#create table call_center
#(
# cc_call_center_sk integer not null,
# cc_call_center_id char(16) not null,
# cc_rec_start_date date,
# cc_rec_end_date date,
# cc_closed_date_sk integer,
# cc_open_date_sk integer,
# cc_name varchar(50),
# cc_class varchar(50),
# cc_employees integer,
# cc_sq_ft integer,
# cc_hours char(20),
# cc_manager varchar(40),
# cc_mkt_id integer,
# cc_mkt_class char(50),
# cc_mkt_desc varchar(100),
# cc_market_manager varchar(40),
# cc_division integer,
# cc_division_name varchar(50),
# cc_company integer,
# cc_company_name char(50),
# cc_street_number char(10),
# cc_street_name varchar(60),
# cc_street_type char(15),
# cc_suite_number char(10),
# cc_city varchar(60),
# cc_county varchar(30),
# cc_state char(2),
# cc_zip char(10),
# cc_country varchar(20),
# cc_gmt_offset decimal(5,2),
# cc_tax_percentage decimal(5,2),
# primary key (cc_call_center_sk)
#);
#create table customer
#(
# c_customer_sk integer not null,
# c_customer_id char(16) not null,
# c_current_cdemo_sk integer,
# c_current_hdemo_sk integer,
# c_current_addr_sk integer,
# c_first_shipto_date_sk integer,
# c_first_sales_date_sk integer,
# c_salutation char(10),
# c_first_name char(20),
# c_last_name char(30),
# c_preferred_cust_flag char(1),
# c_birth_day integer,
# c_birth_month integer,
# c_birth_year integer,
# c_birth_country varchar(20),
# c_login char(13),
# c_email_address char(50),
# c_last_review_date_sk integer,
# primary key (c_customer_sk)
#);
#create table web_site
#(
# web_site_sk integer not null,
# web_site_id char(16) not null,
# web_rec_start_date date,
# web_rec_end_date date,
# web_name varchar(50),
# web_open_date_sk integer,
# web_close_date_sk integer,
# web_class varchar(50),
# web_manager varchar(40),
# web_mkt_id integer,
# web_mkt_class varchar(50),
# web_mkt_desc varchar(100),
# web_market_manager varchar(40),
# web_company_id integer,
# web_company_name char(50),
# web_street_number char(10),
# web_street_name varchar(60),
# web_street_type char(15),
# web_suite_number char(10),
# web_city varchar(60),
# web_county varchar(30),
# web_state char(2),
# web_zip char(10),
# web_country varchar(20),
# web_gmt_offset decimal(5,2),
# web_tax_percentage decimal(5,2),
# primary key (web_site_sk)
#);
#create table store_returns
#(
# sr_returned_date_sk integer,
# sr_return_time_sk integer,
# sr_item_sk integer not null,
# sr_customer_sk integer,
# sr_cdemo_sk integer,
# sr_hdemo_sk integer,
# sr_addr_sk integer,
# sr_store_sk integer,
# sr_reason_sk integer,
# sr_ticket_number integer not null,
# sr_return_quantity integer,
# sr_return_amt decimal(7,2),
# sr_return_tax decimal(7,2),
# sr_return_amt_inc_tax decimal(7,2),
# sr_fee decimal(7,2),
# sr_return_ship_cost decimal(7,2),
# sr_refunded_cash decimal(7,2),
# sr_reversed_charge decimal(7,2),
# sr_store_credit decimal(7,2),
# sr_net_loss decimal(7,2),
# primary key (sr_item_sk, sr_ticket_number)
#);
#create table household_demographics
#(
# hd_demo_sk integer not null,
# hd_income_band_sk integer,
# hd_buy_potential char(15),
# hd_dep_count integer,
# hd_vehicle_count integer,
# primary key (hd_demo_sk)
#);
#create table web_page
#(
# wp_web_page_sk integer not null,
# wp_web_page_id char(16) not null,
# wp_rec_start_date date,
# wp_rec_end_date date,
# wp_creation_date_sk integer,
# wp_access_date_sk integer,
# wp_autogen_flag char(1),
# wp_customer_sk integer,
# wp_url varchar(100),
# wp_type char(50),
# wp_char_count integer,
# wp_link_count integer,
# wp_image_count integer,
# wp_max_ad_count integer,
# primary key (wp_web_page_sk)
#);
#create table promotion
#(
# p_promo_sk integer not null,
# p_promo_id char(16) not null,
# p_start_date_sk integer,
# p_end_date_sk integer,
# p_item_sk integer,
# p_cost decimal(15,2),
# p_response_target integer,
# p_promo_name char(50),
# p_channel_dmail char(1),
# p_channel_email char(1),
# p_channel_catalog char(1),
# p_channel_tv char(1),
# p_channel_radio char(1),
# p_channel_press char(1),
# p_channel_event char(1),
# p_channel_demo char(1),
# p_channel_details varchar(100),
# p_purpose char(15),
# p_discount_active char(1),
# primary key (p_promo_sk)
#);
#create table catalog_page
#(
# cp_catalog_page_sk integer not null,
# cp_catalog_page_id char(16) not null,
# cp_start_date_sk integer,
# cp_end_date_sk integer,
# cp_department varchar(50),
# cp_catalog_number integer,
# cp_catalog_page_number integer,
# cp_description varchar(100),
# cp_type varchar(100),
# primary key (cp_catalog_page_sk)
#);
#create table inventory
#(
# inv_date_sk integer not null,
# inv_item_sk integer not null,
# inv_warehouse_sk integer not null,
# inv_quantity_on_hand integer,
# primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
#);
#create table catalog_returns
#(
# cr_returned_date_sk integer,
# cr_returned_time_sk integer,
# cr_item_sk integer not null,
# cr_refunded_customer_sk integer,
# cr_refunded_cdemo_sk integer,
# cr_refunded_hdemo_sk integer,
# cr_refunded_addr_sk integer,
# cr_returning_customer_sk integer,
# cr_returning_cdemo_sk integer,
# cr_returning_hdemo_sk integer,
# cr_returning_addr_sk integer,
# cr_call_center_sk integer,
# cr_catalog_page_sk integer,
# cr_ship_mode_sk integer,
# cr_warehouse_sk integer,
# cr_reason_sk integer,
# cr_order_number integer not null,
# cr_return_quantity integer,
# cr_return_amount decimal(7,2),
# cr_return_tax decimal(7,2),
# cr_return_amt_inc_tax decimal(7,2),
# cr_fee decimal(7,2),
# cr_return_ship_cost decimal(7,2),
# cr_refunded_cash decimal(7,2),
# cr_reversed_charge decimal(7,2),
# cr_store_credit decimal(7,2),
# cr_net_loss decimal(7,2),
# primary key (cr_item_sk, cr_order_number)
#);
#create table web_returns
#(
# wr_returned_date_sk integer,
# wr_returned_time_sk integer,
# wr_item_sk integer not null,
# wr_refunded_customer_sk integer,
# wr_refunded_cdemo_sk integer,
# wr_refunded_hdemo_sk integer,
# wr_refunded_addr_sk integer,
# wr_returning_customer_sk integer,
# wr_returning_cdemo_sk integer,
# wr_returning_hdemo_sk integer,
# wr_returning_addr_sk integer,
# wr_web_page_sk integer,
# wr_reason_sk integer,
# wr_order_number integer not null,
# wr_return_quantity integer,
# wr_return_amt decimal(7,2),
# wr_return_tax decimal(7,2),
# wr_return_amt_inc_tax decimal(7,2),
# wr_fee decimal(7,2),
# wr_return_ship_cost decimal(7,2),
# wr_refunded_cash decimal(7,2),
# wr_reversed_charge decimal(7,2),
# wr_account_credit decimal(7,2),
# wr_net_loss decimal(7,2),
# primary key (wr_item_sk, wr_order_number)
#);
#create table web_sales
#(
# ws_sold_date_sk integer,
# ws_sold_time_sk integer,
# ws_ship_date_sk integer,
# ws_item_sk integer not null,
# ws_bill_customer_sk integer,
# ws_bill_cdemo_sk integer,
# ws_bill_hdemo_sk integer,
# ws_bill_addr_sk integer,
# ws_ship_customer_sk integer,
# ws_ship_cdemo_sk integer,
# ws_ship_hdemo_sk integer,
# ws_ship_addr_sk integer,
# ws_web_page_sk integer,
# ws_web_site_sk integer,
# ws_ship_mode_sk integer,
# ws_warehouse_sk integer,
# ws_promo_sk integer,
# ws_order_number integer not null,
# ws_quantity integer,
# ws_wholesale_cost decimal(7,2),
# ws_list_price decimal(7,2),
# ws_sales_price decimal(7,2),
# ws_ext_discount_amt decimal(7,2),
# ws_ext_sales_price decimal(7,2),
# ws_ext_wholesale_cost decimal(7,2),
# ws_ext_list_price decimal(7,2),
# ws_ext_tax decimal(7,2),
# ws_coupon_amt decimal(7,2),
# ws_ext_ship_cost decimal(7,2),
# ws_net_paid decimal(7,2),
# ws_net_paid_inc_tax decimal(7,2),
# ws_net_paid_inc_ship decimal(7,2),
# ws_net_paid_inc_ship_tax decimal(7,2),
# ws_net_profit decimal(7,2),
# primary key (ws_item_sk, ws_order_number)
#);
#create table catalog_sales
#(
# cs_sold_date_sk integer,
# cs_sold_time_sk integer,
# cs_ship_date_sk integer,
# cs_bill_customer_sk integer,
# cs_bill_cdemo_sk integer,
# cs_bill_hdemo_sk integer,
# cs_bill_addr_sk integer,
# cs_ship_customer_sk integer,
# cs_ship_cdemo_sk integer,
# cs_ship_hdemo_sk integer,
# cs_ship_addr_sk integer,
# cs_call_center_sk integer,
# cs_catalog_page_sk integer,
# cs_ship_mode_sk integer,
# cs_warehouse_sk integer,
# cs_item_sk integer not null,
# cs_promo_sk integer,
# cs_order_number integer not null,
# cs_quantity integer,
# cs_wholesale_cost decimal(7,2),
# cs_list_price decimal(7,2),
# cs_sales_price decimal(7,2),
# cs_ext_discount_amt decimal(7,2),
# cs_ext_sales_price decimal(7,2),
# cs_ext_wholesale_cost decimal(7,2),
# cs_ext_list_price decimal(7,2),
# cs_ext_tax decimal(7,2),
# cs_coupon_amt decimal(7,2),
# cs_ext_ship_cost decimal(7,2),
# cs_net_paid decimal(7,2),
# cs_net_paid_inc_tax decimal(7,2),
# cs_net_paid_inc_ship decimal(7,2),
# cs_net_paid_inc_ship_tax decimal(7,2),
# cs_net_profit decimal(7,2),
# primary key (cs_item_sk, cs_order_number)
#);
#create table store_sales
#(
# ss_sold_date_sk integer,
# ss_sold_time_sk integer,
# ss_item_sk integer not null,
# ss_customer_sk integer,
# ss_cdemo_sk integer,
# ss_hdemo_sk integer,
# ss_addr_sk integer,
# ss_store_sk integer,
# ss_promo_sk integer,
# ss_ticket_number integer not null,
# ss_quantity integer,
# ss_wholesale_cost decimal(7,2),
# ss_list_price decimal(7,2),
# ss_sales_price decimal(7,2),
# ss_ext_discount_amt decimal(7,2),
# ss_ext_sales_price decimal(7,2),
# ss_ext_wholesale_cost decimal(7,2),
# ss_ext_list_price decimal(7,2),
# ss_ext_tax decimal(7,2),
# ss_coupon_amt decimal(7,2),
# ss_net_paid decimal(7,2),
# ss_net_paid_inc_tax decimal(7,2),
# ss_net_profit decimal(7,2),
# primary key (ss_item_sk, ss_ticket_number)
#);
#alter table call_center add constraint cc_d1 foreign key (cc_closed_date_sk) references date_dim (d_date_sk);
#alter table call_center add constraint cc_d2 foreign key (cc_open_date_sk) references date_dim (d_date_sk);
#alter table catalog_page add constraint cp_d1 foreign key (cp_end_date_sk) references date_dim (d_date_sk);
#alter table catalog_page add constraint cp_d2 foreign key (cp_start_date_sk) references date_dim (d_date_sk);
#alter table catalog_returns add constraint cr_cc foreign key (cr_call_center_sk) references call_center (cc_call_center_sk);
#alter table catalog_returns add constraint cr_cp foreign key (cr_catalog_page_sk) references catalog_page (cp_catalog_page_sk);
#alter table catalog_returns add constraint cr_i foreign key (cr_item_sk) references item (i_item_sk);
#alter table catalog_returns add constraint cr_r foreign key (cr_reason_sk) references reason (r_reason_sk);
#alter table catalog_returns add constraint cr_a1 foreign key (cr_refunded_addr_sk) references customer_address (ca_address_sk);
#alter table catalog_returns add constraint cr_cd1 foreign key (cr_refunded_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table catalog_returns add constraint cr_c1 foreign key (cr_refunded_customer_sk) references customer (c_customer_sk);
#alter table catalog_returns add constraint cr_hd1 foreign key (cr_refunded_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table catalog_returns add constraint cr_d1 foreign key (cr_returned_date_sk) references date_dim (d_date_sk);
#alter table catalog_returns add constraint cr_t foreign key (cr_returned_time_sk) references time_dim (t_time_sk);
#alter table catalog_returns add constraint cr_a2 foreign key (cr_returning_addr_sk) references customer_address (ca_address_sk);
#alter table catalog_returns add constraint cr_cd2 foreign key (cr_returning_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table catalog_returns add constraint cr_c2 foreign key (cr_returning_customer_sk) references customer (c_customer_sk);
#alter table catalog_returns add constraint cr_hd2 foreign key (cr_returning_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table catalog_returns add constraint cr_sm foreign key (cr_ship_mode_sk) references ship_mode (sm_ship_mode_sk);
#alter table catalog_returns add constraint cr_w2 foreign key (cr_warehouse_sk) references warehouse (w_warehouse_sk);
#alter table catalog_sales add constraint cs_b_a foreign key (cs_bill_addr_sk) references customer_address (ca_address_sk);
#alter table catalog_sales add constraint cs_b_cd foreign key (cs_bill_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table catalog_sales add constraint cs_b_c foreign key (cs_bill_customer_sk) references customer (c_customer_sk);
#alter table catalog_sales add constraint cs_b_hd foreign key (cs_bill_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table catalog_sales add constraint cs_cc foreign key (cs_call_center_sk) references call_center (cc_call_center_sk);
#alter table catalog_sales add constraint cs_cp foreign key (cs_catalog_page_sk) references catalog_page (cp_catalog_page_sk);
#alter table catalog_sales add constraint cs_i foreign key (cs_item_sk) references item (i_item_sk);
#alter table catalog_sales add constraint cs_p foreign key (cs_promo_sk) references promotion (p_promo_sk);
#alter table catalog_sales add constraint cs_s_a foreign key (cs_ship_addr_sk) references customer_address (ca_address_sk);
#alter table catalog_sales add constraint cs_s_cd foreign key (cs_ship_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table catalog_sales add constraint cs_s_c foreign key (cs_ship_customer_sk) references customer (c_customer_sk);
#alter table catalog_sales add constraint cs_d1 foreign key (cs_ship_date_sk) references date_dim (d_date_sk);
#alter table catalog_sales add constraint cs_s_hd foreign key (cs_ship_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table catalog_sales add constraint cs_sm foreign key (cs_ship_mode_sk) references ship_mode (sm_ship_mode_sk);
#alter table catalog_sales add constraint cs_d2 foreign key (cs_sold_date_sk) references date_dim (d_date_sk);
#alter table catalog_sales add constraint cs_t foreign key (cs_sold_time_sk) references time_dim (t_time_sk);
#alter table catalog_sales add constraint cs_w foreign key (cs_warehouse_sk) references warehouse (w_warehouse_sk);
#alter table customer add constraint c_a foreign key (c_current_addr_sk) references customer_address (ca_address_sk);
#alter table customer add constraint c_cd foreign key (c_current_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table customer add constraint c_hd foreign key (c_current_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table customer add constraint c_fsd foreign key (c_first_sales_date_sk) references date_dim (d_date_sk);
#alter table customer add constraint c_fsd2 foreign key (c_first_shipto_date_sk) references date_dim (d_date_sk);
#alter table household_demographics add constraint hd_ib foreign key (hd_income_band_sk) references income_band (ib_income_band_sk);
#alter table inventory add constraint inv_d foreign key (inv_date_sk) references date_dim (d_date_sk);
#alter table inventory add constraint inv_i foreign key (inv_item_sk) references item (i_item_sk);
#alter table inventory add constraint inv_w foreign key (inv_warehouse_sk) references warehouse (w_warehouse_sk);
#alter table promotion add constraint p_end_date foreign key (p_end_date_sk) references date_dim (d_date_sk);
#alter table promotion add constraint p_i foreign key (p_item_sk) references item (i_item_sk);
#alter table promotion add constraint p_start_date foreign key (p_start_date_sk) references date_dim (d_date_sk);
#alter table store add constraint s_close_date foreign key (s_closed_date_sk) references date_dim (d_date_sk);
#alter table store_returns add constraint sr_a foreign key (sr_addr_sk) references customer_address (ca_address_sk);
#alter table store_returns add constraint sr_cd foreign key (sr_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table store_returns add constraint sr_c foreign key (sr_customer_sk) references customer (c_customer_sk);
#alter table store_returns add constraint sr_hd foreign key (sr_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table store_returns add constraint sr_i foreign key (sr_item_sk) references item (i_item_sk);
#alter table store_returns add constraint sr_r foreign key (sr_reason_sk) references reason (r_reason_sk);
#alter table store_returns add constraint sr_ret_d foreign key (sr_returned_date_sk) references date_dim (d_date_sk);
#alter table store_returns add constraint sr_t foreign key (sr_return_time_sk) references time_dim (t_time_sk);
#alter table store_returns add constraint sr_s foreign key (sr_store_sk) references store (s_store_sk);
#alter table store_sales add constraint ss_a foreign key (ss_addr_sk) references customer_address (ca_address_sk);
#alter table store_sales add constraint ss_cd foreign key (ss_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table store_sales add constraint ss_c foreign key (ss_customer_sk) references customer (c_customer_sk);
#alter table store_sales add constraint ss_hd foreign key (ss_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table store_sales add constraint ss_i foreign key (ss_item_sk) references item (i_item_sk);
#alter table store_sales add constraint ss_p foreign key (ss_promo_sk) references promotion (p_promo_sk);
#alter table store_sales add constraint ss_d foreign key (ss_sold_date_sk) references date_dim (d_date_sk);
#alter table store_sales add constraint ss_t foreign key (ss_sold_time_sk) references time_dim (t_time_sk);
#alter table store_sales add constraint ss_s foreign key (ss_store_sk) references store (s_store_sk);
#alter table web_page add constraint wp_ad foreign key (wp_access_date_sk) references date_dim (d_date_sk);
#alter table web_page add constraint wp_cd foreign key (wp_creation_date_sk) references date_dim (d_date_sk);
#alter table web_returns add constraint wr_i foreign key (wr_item_sk) references item (i_item_sk);
#alter table web_returns add constraint wr_r foreign key (wr_reason_sk) references reason (r_reason_sk);
#alter table web_returns add constraint wr_ref_a foreign key (wr_refunded_addr_sk) references customer_address (ca_address_sk);
#alter table web_returns add constraint wr_ref_cd foreign key (wr_refunded_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table web_returns add constraint wr_ref_c foreign key (wr_refunded_customer_sk) references customer (c_customer_sk);
#alter table web_returns add constraint wr_ref_hd foreign key (wr_refunded_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table web_returns add constraint wr_ret_d foreign key (wr_returned_date_sk) references date_dim (d_date_sk);
#alter table web_returns add constraint wr_ret_t foreign key (wr_returned_time_sk) references time_dim (t_time_sk);
#alter table web_returns add constraint wr_ret_a foreign key (wr_returning_addr_sk) references customer_address (ca_address_sk);
#alter table web_returns add constraint wr_ret_cd foreign key (wr_returning_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table web_returns add constraint wr_ret_c foreign key (wr_returning_customer_sk) references customer (c_customer_sk);
#alter table web_returns add constraint wr_ret_hd foreign key (wr_returning_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table web_returns add constraint wr_wp foreign key (wr_web_page_sk) references web_page (wp_web_page_sk);
#alter table web_sales add constraint ws_b_a foreign key (ws_bill_addr_sk) references customer_address (ca_address_sk);
#alter table web_sales add constraint ws_b_cd foreign key (ws_bill_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table web_sales add constraint ws_b_c foreign key (ws_bill_customer_sk) references customer (c_customer_sk);
#alter table web_sales add constraint ws_b_hd foreign key (ws_bill_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table web_sales add constraint ws_i foreign key (ws_item_sk) references item (i_item_sk);
#alter table web_sales add constraint ws_p foreign key (ws_promo_sk) references promotion (p_promo_sk);
#alter table web_sales add constraint ws_s_a foreign key (ws_ship_addr_sk) references customer_address (ca_address_sk);
#alter table web_sales add constraint ws_s_cd foreign key (ws_ship_cdemo_sk) references customer_demographics (cd_demo_sk);
#alter table web_sales add constraint ws_s_c foreign key (ws_ship_customer_sk) references customer (c_customer_sk);
#alter table web_sales add constraint ws_s_d foreign key (ws_ship_date_sk) references date_dim (d_date_sk);
#alter table web_sales add constraint ws_s_hd foreign key (ws_ship_hdemo_sk) references household_demographics (hd_demo_sk);
#alter table web_sales add constraint ws_sm foreign key (ws_ship_mode_sk) references ship_mode (sm_ship_mode_sk);
#alter table web_sales add constraint ws_d2 foreign key (ws_sold_date_sk) references date_dim (d_date_sk);
#alter table web_sales add constraint ws_t foreign key (ws_sold_time_sk) references time_dim (t_time_sk);
#alter table web_sales add constraint ws_w2 foreign key (ws_warehouse_sk) references warehouse (w_warehouse_sk);
#alter table web_sales add constraint ws_wp foreign key (ws_web_page_sk) references web_page (wp_web_page_sk);
#alter table web_sales add constraint ws_ws foreign key (ws_web_site_sk) references web_site (web_site_sk);
#alter table web_site add constraint web_d1 foreign key (web_close_date_sk) references date_dim (d_date_sk);
#alter table web_site add constraint web_d2 foreign key (web_open_date_sk) references date_dim (d_date_sk);
#commit;
#''')
#    sys.stdout.write(out)
#    sys.stderr.write(err)
#
#with process.client('sqldump', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as c:
#    out, err = c.communicate()
#    sys.stdout.write(out)
#    sys.stderr.write(err)
#
#with process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
#    out, err = c.communicate('''
#start transaction;
#drop table if exists dbgen_version cascade;
#drop table if exists customer_address cascade;
#drop table if exists customer_demographics cascade;
#drop table if exists date_dim cascade;
#drop table if exists warehouse cascade;
#drop table if exists ship_mode cascade;
#drop table if exists time_dim cascade;
#drop table if exists reason cascade;
#drop table if exists income_band cascade;
#drop table if exists item cascade;
#drop table if exists store cascade;
#drop table if exists call_center cascade;
#drop table if exists customer cascade;
#drop table if exists web_site cascade;
#drop table if exists store_returns cascade;
#drop table if exists household_demographics cascade;
#drop table if exists web_page cascade;
#drop table if exists promotion cascade;
#drop table if exists catalog_page cascade;
#drop table if exists inventory cascade;
#drop table if exists catalog_returns cascade;
#drop table if exists web_returns cascade;
#drop table if exists web_sales cascade;
#drop table if exists catalog_sales cascade;
#drop table if exists store_sales cascade;
#commit;
#''')
#    sys.stdout.write(out)
#    sys.stderr.write(err)
