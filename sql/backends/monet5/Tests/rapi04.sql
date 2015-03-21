START TRANSACTION;

CREATE TABLE ts (ts INTEGER);
INSERT INTO ts VALUES (1387360138),(451665720),(514382400),(1000209600),(1326272400);

CREATE FUNCTION moon(ts integer,lat float, long float) returns integer language R {
	library(moonsun)
	options(latitude=lat,longitude=long)
	angles <- sapply(ts,FUN=function(x){
		ts <- as.POSIXlt(x,origin = "1970-01-01",tz="UTC")
		moon(jd(epoch=ts)+ts$hour/24)$angle
	})
	return(as.integer(ts[angles > 0]))
};

SELECT * FROM ts WHERE moon(ts,52.3,4.8) > 0;
DROP FUNCTION moon;
DROP TABLE ts;

ROLLBACK;
