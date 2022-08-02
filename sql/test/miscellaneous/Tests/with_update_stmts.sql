create table "testme" ("aa" int, "bb" clob);

with "v1" as (select 1 as "c1"),
     "v2" as (select 'a' as "c2")
     insert into "testme" select "c1", "c2" from "v1", "v2";
select "aa", "bb" from "testme";

with "v1" as (select 1 as "c1"),
     "v2" as (select 'a' as "c2")
     insert into "testme" select "c1" from "v1"; --error
with "v1" as (select 1 as "c1" union select 2 as "c1"),
     "v2" as (select 'a' as "c2")
     insert into "testme" select "c1", "c2" from "v1", "v2";
select "aa", "bb" from "testme";

with "v1" as (select 1 as "c1")
     delete from "testme" where "testme"."aa" = "v1"."c1";
with "v1" as (select 0 as "c1")
     delete from "testme" where "testme"."aa" = "v1"."c3"; --error
with "v1" as (select 0 as "c1")
     delete from "testme" where "testme"."aa" = "v1"."c1";
select "aa", "bb" from "testme";

with "v1" as (select 2 as "c1")
     update "testme" set "aa" = 3 where "testme"."aa" = "v1"."c1";
select "aa", "bb" from "testme";

with "v1" as (select 10 as "c1"),
     "v2" as (select 'zzz' as "c2")
merge into "testme" using (select "c1" "joimne", "c2" from "v1","v2") "other" on "testme"."aa" = "other"."joimne"
      when not matched then insert values ("joimne" * "other"."joimne", "c2");
select "aa", "bb" from "testme";

drop table "testme";
