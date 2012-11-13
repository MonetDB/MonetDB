create aggregate stddev(val TINYINT) returns TINYINT
	external name "aggr"."stddev";
create aggregate stddev(val SMALLINT) returns SMALLINT
	external name "aggr"."stddev";
create aggregate stddev(val INTEGER) returns INTEGER
	external name "aggr"."stddev";
create aggregate stddev(val BIGINT) returns BIGINT
	external name "aggr"."stddev";
create aggregate stddev(val REAL) returns REAL
	external name "aggr"."stddev";
create aggregate stddev(val DOUBLE) returns DOUBLE
	external name "aggr"."stddev";

create aggregate stddev(val DATE) returns DATE
	external name "aggr"."stddev";
create aggregate stddev(val TIME) returns TIME
	external name "aggr"."stddev";
create aggregate stddev(val TIMESTAMP) returns TIMESTAMP
	external name "aggr"."stddev";

create aggregate median(val TINYINT) returns TINYINT
	external name "aggr"."median";
create aggregate median(val SMALLINT) returns SMALLINT
	external name "aggr"."median";
create aggregate median(val INTEGER) returns INTEGER
	external name "aggr"."median";
create aggregate median(val BIGINT) returns BIGINT
	external name "aggr"."median";
create aggregate median(val REAL) returns REAL
	external name "aggr"."median";
create aggregate median(val DOUBLE) returns DOUBLE
	external name "aggr"."median";

create aggregate median(val DATE) returns DATE
	external name "aggr"."median";
create aggregate median(val TIME) returns TIME
	external name "aggr"."median";
create aggregate median(val TIMESTAMP) returns TIMESTAMP
	external name "aggr"."median";

create aggregate corr(e1 TINYINT, e2 TINYINT) returns TINYINT
	external name "aggr"."corr";
create aggregate corr(e1 SMALLINT, e2 SMALLINT) returns SMALLINT
	external name "aggr"."corr";
create aggregate corr(e1 INTEGER, e2 INTEGER) returns INTEGER
	external name "aggr"."corr";
create aggregate corr(e1 BIGINT, e2 BIGINT) returns BIGINT
	external name "aggr"."corr";
create aggregate corr(e1 REAL, e2 REAL) returns REAL
	external name "aggr"."corr";
create aggregate corr(e1 DOUBLE, e2 DOUBLE) returns DOUBLE
	external name "aggr"."corr";
