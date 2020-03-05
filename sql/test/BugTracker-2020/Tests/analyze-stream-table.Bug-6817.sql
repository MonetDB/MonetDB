delete from statistics;
select count(*) from statistics;

CREATE SCHEMA "ttt";

CREATE TABLE "ttt"."a"("id" INTEGER);

CREATE STREAM TABLE "ttt"."strt" (
        "id" INTEGER       NOT NULL,
        "nm" VARCHAR(123)  NOT NULL,
        CONSTRAINT "strt_id_pkey" PRIMARY KEY ("id")
);

select * from "ttt"."strt";

analyze "ttt"."strt";
-- Error: Table 'strt' is not persistent   SQLState:  42S02
select (count(*) > 0) as has_rows from statistics;

analyze ttt; --not an error, skip stream table "strt"
select (count(*) > 0) as has_rows from statistics;

drop table "ttt"."strt";
-- now run analyze without the stream table
analyze ttt;
select (count(*) > 0) as has_rows from statistics;
-- true
delete from statistics;

drop schema "ttt" cascade;
