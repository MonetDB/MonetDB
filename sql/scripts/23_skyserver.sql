-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2008-2015 MonetDB B.V.

CREATE FUNCTION MS_STUFF( s1 varchar(32), st int, len int, s3 varchar(32))
RETURNS varchar(32)
BEGIN
	DECLARE res varchar(32), aux varchar(32);
	DECLARE ofset int;

    IF ( st < 0 or st > LENGTH(s1))
        THEN RETURN '';
    END IF;

    SET ofset = 1;
    SET res = SUBSTRING(s1,ofset,st-1);
    SET res = res || s3;
    SET ofset = st + len;
    SET aux = SUBSTRING(s1,ofset,LENGTH(s1)-ofset+1);
	SET res = res || aux;
	RETURN res;
END;

grant execute on function MS_STUFF to public;

CREATE FUNCTION MS_TRUNC(num double, prc int)
RETURNS double
external name sql.ms_trunc;

grant execute on function MS_TRUNC to public;

CREATE FUNCTION MS_ROUND(num double, prc int, truncat int)
RETURNS double
BEGIN
	IF (truncat = 0)
		THEN RETURN ROUND(num, prc);
		ELSE RETURN MS_TRUNC(num, prc);
	END IF;
END;

grant execute on function MS_ROUND to public;

CREATE FUNCTION MS_STR(num float, prc int, truncat int)
RETURNS string
BEGIN
        RETURN CAST(num as string);
END;

grant execute on function MS_STR to public;

CREATE FUNCTION alpha(pdec double, pradius double)
RETURNS double EXTERNAL NAME sql.alpha;

grant execute on function alpha to public;
