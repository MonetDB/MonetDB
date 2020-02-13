delete from statistics;
select count(*) from statistics;

CREATE STREAM TABLE "sys"."strt" (
        "id" INTEGER       NOT NULL,
        "nm" VARCHAR(123)  NOT NULL,
        CONSTRAINT "strt_id_pkey" PRIMARY KEY ("id")
);

select * from "sys"."strt";

analyze "sys"."strt";
-- Error: Table 'strt' is not persistent   SQLState:  42S02
select (count(*) > 0) as has_rows from statistics;

analyze sys; --not an error, skip stream table "strt"
select (count(*) > 0) as has_rows from statistics;

drop table "sys"."strt";
-- now run analyze without the stream table
analyze sys;
select (count(*) > 0) as has_rows from statistics;
-- true (181)
delete from statistics;

