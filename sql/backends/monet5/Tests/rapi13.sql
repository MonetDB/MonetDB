start transaction;

CREATE TABLE "streams" (
    "ts"      TIMESTAMP,
    "type"    TINYINT,
    "station" CHARACTER LARGE OBJECT,
    "lat"     DOUBLE,
    "lon"     DOUBLE,
    "alt"     DOUBLE
);

copy 5 records into streams from stdin;
2015-06-03 15:11:17.000000|2|"4CA56B"|52.08069|5.86654|3.8e+04
2015-06-03 15:11:17.000000|2|"4010EA"|51.19084|4.98646|38025
2015-06-03 15:11:17.000000|2|"406C71"|52.36768|7.17085|3.5e+04
2015-06-03 15:11:17.000000|2|"4006A4"|52.44951|5.21294|37025
2015-06-03 15:11:17.000000|2|"45AC45"|52.12491|6.03063|3.6e+04

# three return columns works
CREATE FUNCTION working_test(stt string, tss bigint, lat double, lon double, alt double) returns table (i int, j int, k int) language R {
	return(data.frame(1:10, 1:10, 1:10))
};

# but four does not? something to do with the amount of return columns?
CREATE FUNCTION broken_test(stt string, tss bigint, lat double, lon double, alt double) returns table (i int, j int, k int, l int) language R {
	return(data.frame(1:10, 1:10, 1:10, 1:10))
};

create temporary table planes as SELECT station, (ts-CAST('1970-01-01' AS timestamp)), lat, lon, alt*0.3048 FROM streams WHERE type = 2 and alt > 0 with data;
#this is the input
select * from planes;
select * from working_test( (SELECT * FROM planes AS p) );
select * from broken_test( (SELECT * FROM planes AS p) );

drop table planes;
drop function working_test;
drop function broken_test;

rollback;


