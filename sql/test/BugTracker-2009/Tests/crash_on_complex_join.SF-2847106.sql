CREATE TABLE "sys"."way_nds" (
"way" int,
"idx" int,
"to_node" int
);

CREATE TABLE "sys"."nodes_legacy" (
"id" int NOT NULL,
"long" double,
"lat" double,
"uid" int,
"timestamp" TIMESTAMP WITH TIME ZONE,
"zcurve" bigint
);

select way_nds.way, zcurve from way_nds, nodes_legacy where to_node = id
and zcurve between (zcurve - 4096) and (zcurve + 4096);

drop table nodes_legacy;
drop table way_nds;
