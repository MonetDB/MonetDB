CREATE TABLE "sys"."test2" (
"test1" int,
"test2" int
);

select test from test2 group by test;

drop table "sys"."test2";
