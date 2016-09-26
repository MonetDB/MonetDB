START TRANSACTION;

CREATE TABLE "sys"."test_bug4058" (
       	"f1"   INTEGER       NOT NULL,
       	"f2"     INTEGER       NOT NULL,
       	"f3"   CHARACTER LARGE OBJECT NOT NULL,
       	"f4" BOOLEAN       NOT NULL
);

INSERT INTO "sys"."test_bug4058" VALUES
(81,1310,'V',true),
(303,1205,'V',true),
(601,1502,'V',true),
(839,1312,'A',true),
(408,1302,'G',false),
(665,1410,'V',true),
(267,1604,'A',true),
(556,1208,'G',false),
(386,1409,'G',false),
(831,1606,'A',true),
(681,1509,'G',false),
(784,1511,'A',true),
(777,1503,'A',true),
(781,1407,'V',true),
(682,1402,'V',true),
(796,1507,'V',true),
(743,1505,'V',true),
(715,1409,'G',false),
(769,1402,'A',true),
(760,1511,'A',true);

select * from "sys"."test_bug4058" limit 10;


create table "sys"."test_bug4058_tmp" as ( select * from "sys"."test_bug4058" limit 10);

select * from (select * from "sys"."test_bug4058" where f3='V' AND f4=true) as tmp where f1||f2 NOT IN (select f1||f2 from "sys"."test_bug4058_tmp"); -- WORKS

select * from (select * from "sys"."test_bug4058" where f1||f2 NOT IN (select f1||f2 from "sys"."test_bug4058_tmp")) as tmp where f3='V' AND f4=true;
-- DOES NOT WORK: dev/sql/backends/monet5/rel_bin.c:702: exp_bin: Assertion `0' failed.

select * from "sys"."test_bug4058" where f3='V' AND f4=true AND f1||f2 NOT IN (select f1||f2 from "sys"."test_bug4058_tmp");
-- DOES NOT WORK: dev/sql/backends/monet5/rel_bin.c:702: exp_bin: Assertion `0' failed.

DROP TABLE "sys"."test_bug4058_tmp";
DROP TABLE "sys"."test_bug4058";

ROLLBACK;

