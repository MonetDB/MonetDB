CREATE TABLE "sys"."way_nds" ( "way" int, "idx" int, "to_node" int );
copy 9 records into way_nds from stdin delimiters ',';
35, 0, 200542
35, 1, 274057218
35, 2, 200550
35, 3, 200551
35, 4, 200553
37, 0, 200511
37, 1, 177231081
37, 2, 200513
37, 3, 177081428
select * from way_nds;
select * from way_nds as t1, way_nds as t2 where t1.way = t2.way and t1.idx = t2.idx-1;
drop table way_nds;
