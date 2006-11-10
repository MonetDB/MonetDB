
CREATE FUNCTION degrees(r double) 
RETURNS double
	RETURN r*180/pi();

CREATE FUNCTION radians(d double) 
RETURNS double
	RETURN d*pi()/180;

