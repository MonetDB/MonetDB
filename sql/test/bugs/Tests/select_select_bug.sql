CREATE TABLE vessels (implicit_timestamp timestamp, mmsi int, lat real, lon real, nav_status tinyint, cog real, sog real, true_heading smallint, rotais smallint);
SELECT (SELECT 1), count(DISTINCT mmsi) FROM vessels;
drop table vessels;


