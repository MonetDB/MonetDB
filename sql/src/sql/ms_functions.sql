CREATE FUNCTION MS_STUFF( s1 varchar(32), st int, len int, s3 varchar(32))
RETURNS varchar(32)
BEGIN
	DECLARE res varchar(32), aux varchar(32);
	DECLARE ofset int;
	SET ofset = 0;
	SET res = SUBSTRING(s1,ofset,st-1);
	SET res = res || s3;
	SET ofset = LENGTH(s1)-len;
	SET aux = SUBSTRING(s1,ofset, len);
	SET res = res || aux;
	RETURN res;
END;

CREATE FUNCTION MS_ROUND(num float, precision int, truncat int)
RETURNS float
BEGIN
        IF (truncat = 0)
                THEN RETURN ROUND(num, precision);
                ELSE RETURN ROUND(FLOOR(num), precision);
        END IF;
END;
