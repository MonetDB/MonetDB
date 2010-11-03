create table t1999354a(ra float, "dec" int);


CREATE FUNCTION f2(deg float, truncat int , precision int)
RETURNS varchar(32)
BEGIN
DECLARE
d float,
nd int,
np int,
q varchar(10),
t varchar(16);
--
SET t = '00:00:00.0';
IF (precision < 1)
THEN SET precision = 1;
END IF;
IF (precision > 10)
THEN SET precision = 10;
END IF;
SET np = 0;
WHILE (np < precision-1) DO
SET t = t||'0';
SET np = np + 1;
END WHILE;
SET d = ABS(deg/15.0);
-- degrees
SET nd = FLOOR(d);
SET q = LTRIM(CAST(nd as varchar(2)));
SET t = MS_STUFF(t,3-LENGTH(q),LENGTH(q), q);
-- minutes
SET d = 60.0 * (d-nd);
SET nd = FLOOR(d);
SET q = LTRIM(CAST(nd as varchar(4)));
SET t = MS_STUFF(t,6-LENGTH(q),LENGTH(q), q);
-- seconds
SET d = MS_ROUND( 60.0 * (d-nd),precision,truncat );
--SET q = LTRIM(STR(d,precision));
SET t = MS_STUFF(t,10+precision-LENGTH(q),LENGTH(q), q);
-- SET d = 60.0 * (d-nd);
-- SET q = LTRIM(STR(d,3));
-- SET t = MS_STUFF(t,13-LENGTH(q),LENGTH(q), q);
--
RETURN(t);
END;

SELECT f2(1,2,3);
--SELECT fIAUFromEq(p.ra,p."dec") FROM PhotoPrimary as p;

SELECT f2(p.ra,8,p."dec") FROM t1999354a as p;

drop function f2;
drop table t1999354a;
