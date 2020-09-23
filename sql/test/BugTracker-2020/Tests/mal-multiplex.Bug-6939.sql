select profiler.getlimit();
select wlc.clock();
select wlc.tick();
select wlr.clock();
select wlr.tick();

start transaction;
CREATE TABLE "t0" ("c0" BIGINT);
INSERT INTO "t0" VALUES (0),(1),(2);
select profiler.getlimit() from t0;
select wlc.clock() from t0;
select wlc.tick() from t0;
select wlr.clock() from t0;
select wlr.tick() from t0;
rollback;
