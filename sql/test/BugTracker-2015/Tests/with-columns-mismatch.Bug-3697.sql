START TRANSACTION;

CREATE TABLE quotes (
"id"     INTEGER       NOT NULL,
"qtime"  TIMESTAMP WITH TIME ZONE NOT NULL,
"sdate"  TIMESTAMP WITH TIME ZONE,
"sym"    VARCHAR(10)   NOT NULL, 
"cur"    VARCHAR(10)   NOT NULL,
"open"   DOUBLE        NOT NULL,
"high"   DOUBLE        NOT NULL,
"low"    DOUBLE        NOT NULL,
"close"  DOUBLE        NOT NULL,
"volume" DOUBLE,
CONSTRAINT "quotes_id_pkey" PRIMARY KEY ("id")
);
WITH cte(qtime, open, close, sdate, id, rnk) as (
  select qtime, sdate, id, row_number() over (partition by sdate order by qtime asc) as rnk
  from quotes where sym='SPY'
) select * from cte where rnk=1;

ROLLBACK;
