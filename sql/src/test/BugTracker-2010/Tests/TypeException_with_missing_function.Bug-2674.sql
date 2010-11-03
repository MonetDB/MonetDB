CREATE TABLE t2674 (ra float NOT NULL, "dec" float NOT NULL);

select fDistanceArcMinEq(213.7849600 , -0.4932472, ra,"dec") as distance_arcmin
from t2674, fGetNearbyObjAllEq(213.7849600 , -0.4932472 , 20.8897421 ) as N;

select fDistanceArcMinEq(213.7849600 , -0.4932472, ra,"dec") as distance_arcmin
from t2674, fGetNearbyObjAllEq(213.7849600 , -0.4932472 , 20.8897421 ) as N;

drop table t2674;


CREATE FUNCTION f2674()
RETURNS float
BEGIN
	DECLARE nx1 float;
	SET nx1  = 1.0;
	RETURN (left_shift(nx1 - 1.0,2));
END;

select f2674();

select f2674();

DROP FUNCTION f2674; 
