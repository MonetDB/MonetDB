-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Copyright August 2008-2010 MonetDB B.V.
-- All Rights Reserved.

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
