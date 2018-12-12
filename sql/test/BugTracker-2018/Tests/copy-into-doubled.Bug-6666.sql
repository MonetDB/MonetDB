create table "t1"("v1" int);
create ordered index "t1_v1" on "sys"."t1"("v1");
drop index "t1_v1";
copy 1 records into "t1" from stdin locked;
1
select count(*) from "t1";
drop table "t1";
