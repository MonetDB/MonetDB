START TRANSACTION;

CREATE TABLE "datetest" (
	"low"  timestamp,
	"high" timestamp
);

INSERT INTO "datetest" VALUES ('2004-12-27 16:29:57.409', '9999-12-31 00:00:00.000');

-- this query should return the one value.  Note that it might legally
-- return no row after about 7994 years from now (2005).  We ignore
-- this inconvenience for now.
select * from datetest where now() > low and now() < high;

-- this should always return the one row.
select * from datetest where low < high;

select max(low), max( CAST (low as timestamp(0))) from datetest;

ROLLBACK;
