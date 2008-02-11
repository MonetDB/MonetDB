
create function "epoch"(sec INT) returns TIMESTAMP
	external name timestamp."epoch";

create function "epoch"(ts TIMESTAMP) returns INT
	external name timestamp."epoch";
