START TRANSACTION;

CREATE SCHEMA "schema1";
CREATE TABLE "schema1"."table1" ("col1" CHAR(32));
INSERT INTO "schema1"."table1" VALUES (NULL);
SELECT * FROM "schema1"."table1";

ALTER TABLE "schema1"."table1" ADD "col2" varchar(40);
ALTER TABLE "schema1"."table1" ADD "col3" varchar(40);
ALTER TABLE "schema1"."table1" ADD "col4" varchar(40);
ALTER TABLE "schema1"."table1" ADD "col5" varchar(40);
ALTER TABLE "schema1"."table1" DROP "col2";
ALTER TABLE "schema1"."table1" DROP "col3";
ALTER TABLE "schema1"."table1" ADD "col2" varchar(40);
ALTER TABLE "schema1"."table1" ADD "col3" varchar(40);

SELECT * FROM "schema1"."table1";

SELECT Columns."number", Columns."name", Columns."type", Columns.type_scale FROM sys.columns as Columns
WHERE Columns.table_id = (SELECT Tables."id" FROM sys.tables as Tables 
                          WHERE Tables.schema_id = (SELECT "schema"."id" FROM sys.schemas as "schema" WHERE "schema"."name" = 'schema1') AND Tables."name" = 'table1') 
ORDER BY Columns."number";

SELECT * FROM "schema1"."table1";

/* This part of the test is added twice to test in both auto-commit and no auto-commit modes */

create table "schema1"."table2" (
 "col1" varchar(32),
 "col2" varchar(32),
 "col3" varchar(32),
 "col4" varchar(32),
 "col5" date,
 "col6" char(32),
 "col7" varchar(32),
 "col8" varchar(32),
 "col9" timestamp,
 "col10" varchar(32),
 "col11" varchar(32),
 "col12" timestamp,
 "col13" bigint,
 "col14" timestamp,
 "col15" date,
 "col16" timestamp);

insert into "schema1"."table2" values ('1','1','1','1',date '1990-10-01','1','1','1',timestamp '2008-03-01 00:00','1','9',timestamp '2011-04-04 04:04',1,timestamp '2014-07-07 07:07',date '1966-12-31',timestamp '2017-10-10 10:10'),
                                      ('1','1','1','1',date '1890-02-20','1','1','1',timestamp '2009-04-02 01:01','4','4',timestamp '2012-05-05 05:05',2,timestamp '2015-08-08 08:08',date '1978-12-21',timestamp '2018-11-11 11:11'),
                                      ('1','1','1','1',date '1790-03-30','1','1','1',timestamp '2010-05-03 02:02','8','7',timestamp '2013-06-06 06:06',3,timestamp '2016-09-09 09:09',date '1987-12-11',timestamp '2019-12-12 00:00');

select columns."number", columns."name", columns."type", columns.type_scale from sys."columns" as columns
where columns.table_id = (select tables."id" from sys."tables" as tables
                          where tables.schema_id = (select "schema"."id" from sys."schemas" as "schema" where "schema"."name" = 'schema1') and tables."name" = 'table2') order by columns."number";

ALTER TABLE "schema1"."table2" ADD COLUMN "col17" varchar(42);
UPDATE "schema1"."table2" SET "col17" = CONVERT("col5", varchar(42));
ALTER TABLE "schema1"."table2" DROP COLUMN "col5" CASCADE;
ALTER TABLE "schema1"."table2" RENAME COLUMN "col17" TO "col5";
ALTER TABLE "schema1"."table2" ADD "col18" varchar(42);

select columns."number", columns."name", columns."type", columns.type_scale from sys."columns" as columns
where columns.table_id = (select tables."id" from sys."tables" as tables
                          where tables.schema_id = (select "schema"."id" from sys."schemas" as "schema" where "schema"."name" = 'schema1') and tables."name" = 'table2') order by columns."number";

ROLLBACK;

CREATE SCHEMA "schema1";

create table "schema1"."table2" (
 "col1" varchar(32),
 "col2" varchar(32),
 "col3" varchar(32),
 "col4" varchar(32),
 "col5" date,
 "col6" char(32),
 "col7" varchar(32),
 "col8" varchar(32),
 "col9" timestamp,
 "col10" varchar(32),
 "col11" varchar(32),
 "col12" timestamp,
 "col13" bigint,
 "col14" timestamp,
 "col15" date,
 "col16" timestamp);

insert into "schema1"."table2" values ('1','1','1','1',date '1990-10-01','1','1','1',timestamp '2008-03-01 00:00','1','9',timestamp '2011-04-04 04:04',1,timestamp '2014-07-07 07:07',date '1966-12-31',timestamp '2017-10-10 10:10'),
                                      ('1','1','1','1',date '1890-02-20','1','1','1',timestamp '2009-04-02 01:01','4','4',timestamp '2012-05-05 05:05',2,timestamp '2015-08-08 08:08',date '1978-12-21',timestamp '2018-11-11 11:11'),
                                      ('1','1','1','1',date '1790-03-30','1','1','1',timestamp '2010-05-03 02:02','8','7',timestamp '2013-06-06 06:06',3,timestamp '2016-09-09 09:09',date '1987-12-11',timestamp '2019-12-12 00:00');

select columns."number", columns."name", columns."type", columns.type_scale from sys."columns" as columns
where columns.table_id = (select tables."id" from sys."tables" as tables
                          where tables.schema_id = (select "schema"."id" from sys."schemas" as "schema" where "schema"."name" = 'schema1') and tables."name" = 'table2') order by columns."number";

ALTER TABLE "schema1"."table2" ADD COLUMN "col17" varchar(42);
UPDATE "schema1"."table2" SET "col17" = CONVERT("col5", varchar(42));
ALTER TABLE "schema1"."table2" DROP COLUMN "col5" CASCADE;
ALTER TABLE "schema1"."table2" RENAME COLUMN "col17" TO "col5";
ALTER TABLE "schema1"."table2" ADD "col18" varchar(42);

select columns."number", columns."name", columns."type", columns.type_scale from sys."columns" as columns
where columns.table_id = (select tables."id" from sys."tables" as tables
                          where tables.schema_id = (select "schema"."id" from sys."schemas" as "schema" where "schema"."name" = 'schema1') and tables."name" = 'table2') order by columns."number";

DROP SCHEMA "schema1" CASCADE;
