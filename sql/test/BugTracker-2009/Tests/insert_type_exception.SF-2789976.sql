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

CREATE TABLE "sys"."mytemp" (
"row" int,
"way" int,
"long1" double,
"lat1" double,
"long2" double,
"lat2" double
);

insert into mytemp select row_number() over (order by way) as row, way,
n1.long, n1.lat, n2.long, n2.lat from nodes_legacy as n1, nodes_legacy as
n2, segments where node1 = n1.id and node2 = n2.id;

drop table mytemp;
drop table segments;
drop table nodes_legacy;
