START TRANSACTION;

CREATE TABLE "tm_bug" (
    "uid" integer NOT NULL,
    "weight" double NOT NULL default 1.0,
    "filter" integer NOT NULL default -1
);

INSERT INTO "tm_bug" VALUES (1,1.0,1);
INSERT INTO "tm_bug" VALUES (2,1.0,1);
INSERT INTO "tm_bug" VALUES (3,1.0,2);

select count(uid) as cnt1,stddev_pop(weight) as f1 from tm_bug where filter = 1;
select count(uid) as cnt2,stddev_pop(weight) as f2 from tm_bug where filter = 2;
select count(uid) as cnt3,stddev_pop(weight) as f3 from tm_bug where filter = 3;

ROLLBACK;
