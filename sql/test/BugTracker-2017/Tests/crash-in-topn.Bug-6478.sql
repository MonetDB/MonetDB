CREATE TABLE "sys"."unitTestDontDelete" (
	"A"                VARCHAR(255),
	"B"                BIGINT,
	"C"                DOUBLE,
	"D"                TIMESTAMP,
	"id" BIGINT        NOT NULL,
	CONSTRAINT "\"unitTestDontDelete\"_PK" PRIMARY KEY ("id")
);
INSERT INTO "sys"."unitTestDontDelete" VALUES (NULL, NULL, NULL, NULL, 0);
INSERT INTO "sys"."unitTestDontDelete" VALUES ('Cat1', 0, 0.5, '2013-06-10 11:10:10.000000', 1);
INSERT INTO "sys"."unitTestDontDelete" VALUES ('Cat2', 1, 1.5, '2013-06-11 12:11:11.000000', 2);
INSERT INTO "sys"."unitTestDontDelete" VALUES ('Cat1', 2, 2.5, '2013-06-12 13:12:12.000000', 3);
INSERT INTO "sys"."unitTestDontDelete" VALUES ('Cat2', 3, 3.5, '2013-06-13 14:13:13.000000', 4);
INSERT INTO "sys"."unitTestDontDelete" VALUES ('Cat1', 4, 4.5, '2013-06-14 15:14:14.000000', 5);
INSERT INTO "sys"."unitTestDontDelete" VALUES ('Cat2', 5, 5.5, '2013-06-15 16:15:15.000000', 6);
INSERT INTO "sys"."unitTestDontDelete" VALUES ('Cat1', 6, 6.5, '2013-06-16 17:16:16.000000', 7);
INSERT INTO "sys"."unitTestDontDelete" VALUES ('Cat2', 7, 7.5, '2013-06-17 18:17:17.000000', 8);
INSERT INTO "sys"."unitTestDontDelete" VALUES ('Cat1', 8, 8.5, '2013-06-18 19:18:18.000000', 9);

select "A" as "cb_a", "B" as "cc_b", "C" as "cd_c", "D" as "ce_d" from (
	select "t8"."A", "t8"."B", "t8"."C", "t8"."D", "t8"."id" from (
		select "t7"."A", "t7"."B", "t7"."C", "t7"."D", "t7"."id" from "unitTestDontDelete" as "t7"
	) as "t8" order by "A" desc, "id" asc limit 4 offset 1
) as "ta" order by "B" asc, "A" desc, "id" asc limit 1;

drop table sys."unitTestDontDelete";
