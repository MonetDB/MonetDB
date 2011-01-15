create table test2 (bla1 double, bla2 double);
insert into test2 values (1,1);
insert into test2 values (1,2);
insert into test2 values (1,3);
select case when (bla1 - bla2) > 0 then 1/(bla1 - bla2) else 0 end from test2;
select case when (bla1 - bla2) > 0 then 1/(bla1 - bla2) else 0 end from test2;
drop table test2;

CREATE TABLE "sys"."nodes_legacy" (
"id" int,
"long" double,
"lat" double,
"uid" int,
"timestamp" timestamptz(7)
);

CREATE TABLE "sys"."segments" (
"way" int,
"node1" int,
"node2" int
);

select n1.lat, n1.long, n2.lat, n2.long, case when (n2.lat - n1.lat) > 0
then (n2.long - n1.long)/(n2.lat - n1.lat) else 0 end from nodes_legacy as
n1, nodes_legacy as n2, segments where node1 = n1.id and node2 = n2.id
limit 10;

drop table "sys"."segments";
drop table "sys"."nodes_legacy";

select case when (2 - 2) > 0 then (3 - 2)/(2 - 2) else 0 end as "test";
