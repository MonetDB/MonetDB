CREATE TABLE "sys"."test_fn" (
	"c1" CHARACTER LARGE OBJECT
);

CREATE TABLE "sys"."chg" (
	"pk_chg_rate"        CHARACTER LARGE OBJECT NOT NULL,
	"c_crt_cde"          CHARACTER LARGE OBJECT NOT NULL DEFAULT 'USER ',
	"t_crt_tm"           TIMESTAMP     DEFAULT current_timestamp(),
	"c_upd_cde"          CHARACTER LARGE OBJECT NOT NULL DEFAULT 'USER ',
	"t_upd_tm"           TIMESTAMP     DEFAULT current_timestamp(),
	"c_cur_no_1"         CHARACTER LARGE OBJECT NOT NULL,
	"c_cur_no_2"         CHARACTER LARGE OBJECT NOT NULL,
	"t_effc_tm"          TIMESTAMP     NOT NULL,
	"n_chg_rte"          DECIMAL(13,6) NOT NULL,
	"t_expd_tm"          TIMESTAMP,
	"business_bgn_date"  TIMESTAMP     NOT NULL,
	"business_end_date"  TIMESTAMP     NOT NULL,
	"record_bgn_date"    TIMESTAMP     NOT NULL,
	"record_end_date"    TIMESTAMP     NOT NULL,
	"source_system_flag" CHARACTER LARGE OBJECT,
	CONSTRAINT "chg_pk_chg_rate_pkey" PRIMARY KEY ("pk_chg_rate")
);

copy 4 records into sys.chg from stdin;
"1"|"admin"|2004-05-27 21:55:03.000000|"admin"|2004-05-27 21:55:03.000000|"01"|"02"|2004-01-01 00:00:00.000000|0.938350|2004-12-31 23:59:59.000000|2004-05-27 21:55:03.000000|9999-12-31 00:00:00.000000|2010-10-18 12:39:39.000000|9999-12-31 00:00:00.000000|"1"
"2"|"admin"|2004-05-27 21:57:14.000000|"admin"|2004-05-27 21:57:14.000000|"02"|"01"|2004-01-01 00:00:00.000000|1.065700|2004-12-31 23:59:59.000000|2004-05-27 21:57:14.000000|9999-12-31 00:00:00.000000|2010-10-18 12:39:39.000000|9999-12-31 00:00:00.000000|"1"
"3"|"admin"|2004-05-27 21:58:37.000000|"admin"|2004-05-27 21:58:37.000000|"13"|"03"|2004-01-01 00:00:00.000000|1.249085|2004-12-31 23:59:59.000000|2004-05-27 21:58:37.000000|9999-12-31 00:00:00.000000|2010-10-18 12:39:39.000000|9999-12-31 00:00:00.000000|"1"
"4"|"admin"|2004-05-27 21:59:02.000000|"admin"|2004-05-27 21:59:02.000000|"01"|"03"|2004-01-01 00:00:00.000000|0.120821|2004-12-31 23:59:59.000000|2004-05-27 21:59:02.000000|9999-12-31 00:00:00.000000|2010-10-18 12:39:39.000000|9999-12-31 00:00:00.000000|"1"

copy 4 records into sys.test_fn from stdin;
"01"
"01"
"01"
"01"

CREATE INDEX "chg_c_cur_no_1" ON "sys"."chg" ("c_cur_no_1");
CREATE INDEX "chg_c_cur_no_2" ON "sys"."chg" ("c_cur_no_2");
CREATE INDEX "chg_record_end_date" ON "sys"."chg" ("record_end_date");
--CREATE INDEX "chg_t_effc_tm" ON "sys"."ods_chg" ("t_effc_tm");
CREATE INDEX "chg_t_expd_tm" ON "sys"."chg" ("t_expd_tm");

CREATE FUNCTION Get_r(in_cur_1 text, in_cur_2 text, in_chg_date timestamp)
RETURNS NUMERIC(13,6) 

--  v_chg_date  timestamp;

--  case when in_chg_date is not null then now() else in_chg_date end

--  case when in_cur_1 is not null then '1' else 'xx' end

--  case when in_cur_2 is null then '01' else 'xx' end

  RETURN
  (
    SELECT (case when MAX(n_chg_rte) is not null then MAX(n_chg_rte) else 0 end)
  FROM chg
--    WHERE BUSINESS_END_DATE='99991231';
      WHERE c_cur_no_1 = (case when in_cur_1 is not null then '01' else 'xx' end)
          AND   c_cur_no_2 = (case when in_cur_2 is null then '01' else (case when LENGTH(trim(in_cur_2))=2 then trim(in_cur_2) else 'xx' end) end)
          AND   (case when in_chg_date is null then now() else in_chg_date end) BETWEEN t_effc_tm AND t_expd_tm
          AND BUSINESS_END_DATE='9999-12-31'
  );

select count(*) from test_fn where get_r(c1, '01', '2015-08-31 23:59:59')>1.0;

alter table sys.chg drop constraint chg_pk_chg_rate_pkey;
drop index "chg_c_cur_no_1";
drop index "chg_c_cur_no_2";
drop index "chg_record_end_date";
--drop index "chg_t_effc_tm";
drop index "chg_t_expd_tm";
drop function get_r;
drop table sys.test_fn;
drop table sys.chg;
