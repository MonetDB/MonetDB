CREATE TABLE "sys"."unitTestDontDelete" (
	"A" VARCHAR(255),
	"B" BIGINT,
	"C" DOUBLE,
	"D" TIMESTAMP
);
COPY 10 RECORDS INTO "sys"."unitTestDontDelete" FROM stdin USING DELIMITERS '\t','\n','"';
NULL	NULL	NULL	NULL
"Cat1"	0	0.5	2013-06-10 11:10:10.000000
"Cat2"	1	1.5	2013-06-11 12:11:11.000000
"Cat1"	2	2.5	2013-06-12 13:12:12.000000
"Cat2"	3	3.5	2013-06-13 14:13:13.000000
"Cat1"	4	4.5	2013-06-14 15:14:14.000000
"Cat2"	5	5.5	2013-06-15 16:15:15.000000
"Cat1"	6	6.5	2013-06-16 17:16:16.000000
"Cat2"	7	7.5	2013-06-17 18:17:17.000000
"Cat1"	8	8.5	2013-06-18 19:18:18.000000


CREATE TABLE "sys"."test_join_left_table" (
	"a"   VARCHAR(255),
	"b"   VARCHAR(255),
	"l_c" VARCHAR(255)
);
COPY 4 RECORDS INTO "sys"."test_join_left_table" FROM stdin USING DELIMITERS '\t','\n','"';
"a1"	"b1"	"c1"
"a2"	"b2"	"c2"
"a3"	"b3"	"c3"
"a4"	"b4"	NULL



select * from "test_join_left_table" as "ta" where not exists 
	(select 1 as "one" from 
		(select "t2"."A", "t2"."B", "t2"."C", "t2"."D", "t2"."l_c" from 
		(
			(select "t0"."A" as "A", cast("B" as  CLOB) as "B", "t0"."C" as "C", "t0"."D" as "D", null as "l_c" from "unitTestDontDelete" as "t0")
		union all 
		 	(select "t1"."a" as "A", "t1"."b" as "B", cast(null as double) as "C", cast(null as timestamp) as "D", "t1"."l_c" as "l_c" from "test_join_left_table" as "t1")
		) as "t2") as "tb" 
	where ("ta"."l_c" = "tb"."l_c" or ("ta"."l_c" is null and "tb"."l_c" is null)));

drop table test_join_left_table;
drop table "unitTestDontDelete";



	
