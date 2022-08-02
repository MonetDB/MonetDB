START TRANSACTION;

CREATE TABLE "unitTestDontDelete" (
        "A" VARCHAR(255),
        "B" BIGINT,
        "C" DOUBLE,
        "D" TIMESTAMP
);
INSERT INTO "unitTestDontDelete" VALUES (NULL, NULL, NULL, NULL);
INSERT INTO "unitTestDontDelete" VALUES ('Cat1', 0, 0.5, '2013-06-10 11:10:10.000000');
INSERT INTO "unitTestDontDelete" VALUES ('Cat2', 1, 1.5, '2013-06-11 12:11:11.000000');
INSERT INTO "unitTestDontDelete" VALUES ('Cat1', 2, 2.5, '2013-06-12 13:12:12.000000');
INSERT INTO "unitTestDontDelete" VALUES ('Cat2', 3, 3.5, '2013-06-13 14:13:13.000000');
INSERT INTO "unitTestDontDelete" VALUES ('Cat1', 4, 4.5, '2013-06-14 15:14:14.000000');
INSERT INTO "unitTestDontDelete" VALUES ('Cat2', 5, 5.5, '2013-06-15 16:15:15.000000');
INSERT INTO "unitTestDontDelete" VALUES ('Cat1', 6, 6.5, '2013-06-16 17:16:16.000000');
INSERT INTO "unitTestDontDelete" VALUES ('Cat2', 7, 7.5, '2013-06-17 18:17:17.000000');
INSERT INTO "unitTestDontDelete" VALUES ('Cat1', 8, 8.5, '2013-06-18 19:18:18.000000');

select max("A") from "unitTestDontDelete" where "A" ilike ('%' || '' || '%');

ROLLBACK;
