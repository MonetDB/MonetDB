--
-- TIMESTAMPTZ
--
-- needed so tests pass even in Australia
/* SET australian_timezones = 'off'; */

CREATE TABLE TIMESTAMPTZ_TBL ( d1 timestamp(2) with time zone);
DECLARE test_now timestamp(2) with time zone;
DECLARE test_current_date date;

SET test_now = now;
SET test_current_date = current_date;

--INSERT INTO TIMESTAMPTZ_TBL VALUES ('now');
INSERT INTO TIMESTAMPTZ_TBL VALUES (test_now);
--INSERT INTO TIMESTAMPTZ_TBL VALUES ('current');
INSERT INTO TIMESTAMPTZ_TBL VALUES (test_now);
--INSERT INTO TIMESTAMPTZ_TBL VALUES ('today');
INSERT INTO TIMESTAMPTZ_TBL VALUES (cast(test_current_date as timestamptz));
--INSERT INTO TIMESTAMPTZ_TBL VALUES ('yesterday');
INSERT INTO TIMESTAMPTZ_TBL VALUES (cast(sql_sub(test_current_date, 24*60*60.0) as timestamptz));
--INSERT INTO TIMESTAMPTZ_TBL VALUES ('tomorrow');
INSERT INTO TIMESTAMPTZ_TBL VALUES (cast(sql_add(test_current_date, 24*60*60.0) as timestamptz));

--SELECT d1 FROM TIMESTAMPTZ_TBL;
SELECT count(*) AS One FROM TIMESTAMPTZ_TBL WHERE d1 = cast(test_current_date as timestamptz);
SELECT count(*) AS One FROM TIMESTAMPTZ_TBL WHERE d1 = cast(sql_add(test_current_date, 24*60*60.0)as timestamptz);
SELECT count(*) AS One FROM TIMESTAMPTZ_TBL WHERE d1 = cast(sql_sub(test_current_date, 24*60*60.0)as timestamptz);
SELECT count(*) AS None FROM TIMESTAMPTZ_TBL WHERE d1 = cast(test_now as timestamptz);

INSERT INTO TIMESTAMPTZ_TBL VALUES ('tomorrow EST');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('tomorrow zulu');

DELETE FROM TIMESTAMPTZ_TBL;

-- verify uniform transaction time within transaction block
START TRANSACTION;
INSERT INTO TIMESTAMPTZ_TBL VALUES (test_now);
INSERT INTO TIMESTAMPTZ_TBL VALUES (test_now);
SELECT count(*) AS two FROM TIMESTAMPTZ_TBL WHERE d1 <= cast(test_now as timestamptz);
COMMIT;
DELETE FROM TIMESTAMPTZ_TBL;

-- Special values
INSERT INTO TIMESTAMPTZ_TBL VALUES ('-infinity');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('infinity');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('epoch');
-- Obsolete special values
INSERT INTO TIMESTAMPTZ_TBL VALUES ('invalid');

-- Postgres v6.0 standard output format
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Mon Feb 10 17:32:01 1997 PST');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Invalid Abstime');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Undefined Abstime');

-- Variations on Postgres v6.1 standard output format
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Mon Feb 10 17:32:01.000001 1997 PST');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Mon Feb 10 17:32:01.999999 1997 PST');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Mon Feb 10 17:32:01.4 1997 PST');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Mon Feb 10 17:32:01.5 1997 PST');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Mon Feb 10 17:32:01.6 1997 PST');

-- ISO 8601 format
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-01-02');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-01-02 03:04:05');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-02-10 17:32:01-08');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-02-10 17:32:01-0800');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-02-10 17:32:01 -08:00');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('19970210 173201 -0800');  -- incorrect format
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-06-10 17:32:01 -07:00');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2001-09-22T18:19:20');

-- POSIX format
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2000-03-15 08:14:01 GMT+8');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2000-03-15 13:14:02 GMT-1');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2000-03-15 12:14:03 GMT -2');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2000-03-15 03:14:04 EST+3');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2000-03-15 02:14:05 EST +2:00');

-- Variations for acceptable input formats
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 10 17:32:01 1997 -0800');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 10 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 10 5:32PM 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997/02/10 17:32:01-0800');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-02-10 17:32:01 PST');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb-10-1997 17:32:01 PST');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('02-10-1997 17:32:01 PST');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('19970210 173201 PST');
/* set datestyle to ymd; */
INSERT INTO TIMESTAMPTZ_TBL VALUES ('97FEB10 5:32:01PM UTC');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('97/02/10 17:32:01 UTC');
/* reset datestyle; */
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997.041 17:32:01 UTC');

-- Check date conversion and date arithmetic
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-06-10 18:32:01 PDT');

INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 10 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 11 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 12 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 13 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 14 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 15 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1997');

INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 0097 BC');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 0097');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 0597');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1097');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1697');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1797');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1897');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 2097');

INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 28 17:32:01 1996');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 29 17:32:01 1996');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Mar 01 17:32:01 1996');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 30 17:32:01 1996');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 31 17:32:01 1996');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Jan 01 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 28 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 29 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Mar 01 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 30 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 31 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 31 17:32:01 1999');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Jan 01 17:32:01 2000');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 31 17:32:01 2000');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Jan 01 17:32:01 2001');

-- Currently unsupported syntax and ranges
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 -0097');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 5097 BC');

INSERT INTO TIMESTAMPTZ_TBL VALUES (null);

SET TIME ZONE INTERVAL '+01:00' HOUR TO MINUTE;
SELECT '' AS "17", d1 FROM TIMESTAMPTZ_TBL;

SET TIME ZONE INTERVAL '+04:30' HOUR TO MINUTE;
SELECT d1 AS "Timestamp TZ" FROM TIMESTAMPTZ_TBL ORDER BY 1;

SET TIME ZONE INTERVAL '+14:00' HOUR TO MINUTE;
SELECT d1 AS "Timestamp TZ" FROM TIMESTAMPTZ_TBL ORDER BY 1;

SET TIME ZONE INTERVAL '-07:00' HOUR TO MINUTE;
SELECT d1 AS "Timestamp TZ" FROM TIMESTAMPTZ_TBL ORDER BY 1;

SET TIME ZONE INTERVAL '+01:00' HOUR TO MINUTE;


-- Demonstrate functions and operators
SELECT '' AS "48", d1 FROM TIMESTAMPTZ_TBL
   WHERE d1 > cast('1997-01-02' as timestamptz);

SELECT '' AS "15", d1 FROM TIMESTAMPTZ_TBL
   WHERE d1 < cast('1997-01-02' as timestamptz);

SELECT '' AS one, d1 FROM TIMESTAMPTZ_TBL
   WHERE d1 = cast('1997-01-02' as timestamptz);

SELECT '' AS "63", d1 FROM TIMESTAMPTZ_TBL
   WHERE d1 <> cast('1997-01-02' as timestamptz);

SELECT '' AS "16", d1 FROM TIMESTAMPTZ_TBL
   WHERE d1 <= cast('1997-01-02' as timestamptz);

SELECT '' AS "49", d1 FROM TIMESTAMPTZ_TBL
   WHERE d1 >= cast('1997-01-02' as timestamptz);

SELECT '' AS "54", d1 - cast('1997-01-02' as timestamptz) AS diff
   FROM TIMESTAMPTZ_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01';

SELECT '' AS date_trunc_week, date_trunc( 'week', cast('2004-02-29 15:44:17.71393' as timestamptz) ) AS week_trunc;

-- Test casting within a BETWEEN qualifier
SELECT '' AS "54", d1 - cast('1997-01-02' as timestamptz) AS diff
  FROM TIMESTAMPTZ_TBL
  WHERE d1 BETWEEN cast('1902-01-01' as timestamptz) AND cast('2038-01-01' as timestamptz);

SELECT '' AS "54", d1 as timestamptz,
   date_part( 'year', d1) AS "year", date_part( 'month', d1) AS "month",
   date_part( 'day', d1) AS "day", date_part( 'hour', d1) AS "hour",
   date_part( 'minute', d1) AS "minute", date_part( 'second', d1) AS "second"
   FROM TIMESTAMPTZ_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01';

SELECT '' AS "54", d1 as timestamptz,
   date_part( 'quarter', d1) AS quarter, date_part( 'msec', d1) AS msec,
   date_part( 'usec', d1) AS usec
   FROM TIMESTAMPTZ_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01';

-- TO_CHAR()
SELECT '' AS to_char_1, to_char(d1, 'DAY Day day DY Dy dy MONTH Month month RM MON Mon mon') 
   FROM TIMESTAMPTZ_TBL;
	
SELECT '' AS to_char_2, to_char(d1, 'FMDAY FMDay FMday FMMONTH FMMonth FMmonth FMRM')
   FROM TIMESTAMPTZ_TBL;	

SELECT '' AS to_char_3, to_char(d1, 'Y,YYY YYYY YYY YY Y CC Q MM WW DDD DD D J')
   FROM TIMESTAMPTZ_TBL;
	
SELECT '' AS to_char_4, to_char(d1, 'FMY,YYY FMYYYY FMYYY FMYY FMY FMCC FMQ FMMM FMWW FMDDD FMDD FMD FMJ') 
   FROM TIMESTAMPTZ_TBL;	
	
SELECT '' AS to_char_5, to_char(d1, 'HH HH12 HH24 MI SS SSSS') 
   FROM TIMESTAMPTZ_TBL;

SELECT '' AS to_char_6, to_char(d1, E'"HH:MI:SS is" HH:MI:SS "\\"text between quote marks\\""') 
   FROM TIMESTAMPTZ_TBL;		
		
SELECT '' AS to_char_7, to_char(d1, 'HH24--text--MI--text--SS')
   FROM TIMESTAMPTZ_TBL;		

SELECT '' AS to_char_8, to_char(d1, 'YYYYTH YYYYth Jth') 
   FROM TIMESTAMPTZ_TBL;
  
SELECT '' AS to_char_9, to_char(d1, 'YYYY A.D. YYYY a.d. YYYY bc HH:MI:SS P.M. HH:MI:SS p.m. HH:MI:SS pm') 
   FROM TIMESTAMPTZ_TBL;

SELECT '' AS to_char_10, to_char(d1, 'YYYY WW IYYY IYY IY I IW') 
   FROM TIMESTAMPTZ_TBL;

-- TO_TIMESTAMP()
SELECT '' AS to_timestamp_1, to_timestamp('0097/Feb/16 --> 08:14:30', 'YYYY/Mon/DD --> HH:MI:SS');
	
SELECT '' AS to_timestamp_2, to_timestamp('97/2/16 8:14:30', 'FMYYYY/FMMM/FMDD FMHH:FMMI:FMSS');

SELECT '' AS to_timestamp_3, to_timestamp('1985 January 12', 'YYYY FMMonth DD');

SELECT '' AS to_timestamp_4, to_timestamp('My birthday-> Year: 1976, Month: May, Day: 16',
										  '"My birthday-> Year" YYYY, "Month:" FMMonth, "Day:" DD');

SELECT '' AS to_timestamp_5, to_timestamp('1,582nd VIII 21', 'Y,YYYth FMRM DD');

SELECT '' AS to_timestamp_6, to_timestamp('15 "text between quote marks" 98 54 45', 
										  E'HH "\\text between quote marks\\"" YY MI SS');
    
SELECT '' AS to_timestamp_7, to_timestamp('05121445482000', 'MMDDHHMISSYYYY');

SELECT '' AS to_timestamp_8, to_timestamp('2000January09Sunday', 'YYYYFMMonthDDFMDay');

SELECT '' AS to_timestamp_9, to_timestamp('97/Feb/16', 'YYMonDD');

SELECT '' AS to_timestamp_10, to_timestamp('19971116', 'YYYYMMDD');

SELECT '' AS to_timestamp_11, to_timestamp('20000-1116', 'YYYY-MMDD');

SELECT '' AS to_timestamp_12, to_timestamp('9-1116', 'Y-MMDD');

SELECT '' AS to_timestamp_13, to_timestamp('95-1116', 'YY-MMDD');

SELECT '' AS to_timestamp_14, to_timestamp('995-1116', 'YYY-MMDD');

SELECT '' AS to_timestamp_15, to_timestamp('200401', 'IYYYIW');

SELECT '' AS to_timestamp_16, to_timestamp('200401', 'YYYYWW');


/* SET DateStyle TO DEFAULT; */

-- test MonetDB date functions
-- select distinct name, func, mod, language, type, schema_id from sys.functions where id in (select func_id from sys.args where number in (0, 1) and name in ('res_0', 'arg_1') and type = 'timestamptz') order by name, func, schema_id
-- select current_timestamp();  -- retuns timestamptz
-- select localtimestamp();     -- retuns timestamp
-- select current_timestamp;  -- retuns timestamptz
-- select localtimestamp;     -- retuns timestamp

SELECT d1, day(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, "day"(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, month(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, "month"(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, year(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, "year"(d1) FROM TIMESTAMPTZ_TBL;

SELECT d1, extract(day from d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, extract(week from d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, extract(month from d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, extract(quarter from d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, extract(halfyear from d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, extract(year from d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, extract(century from d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, extract(millennium from d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, extract(epoch from d1) FROM TIMESTAMPTZ_TBL;

SELECT d1, week(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, weekofyear(d1) FROM TIMESTAMPTZ_TBL;

SELECT d1, dayofmonth(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, dayofweek(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, dayofyear(d1) FROM TIMESTAMPTZ_TBL;

SELECT d1, hour(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, "hour"(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, minute(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, "minute"(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, second(d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, "second"(d1) FROM TIMESTAMPTZ_TBL;

SELECT d1, extract(hour from d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, extract(minute from d1) FROM TIMESTAMPTZ_TBL;
SELECT d1, extract(second from d1) FROM TIMESTAMPTZ_TBL;

SELECT d1, sql_add(d1, 365*24*60*60.0) FROM TIMESTAMPTZ_TBL;
SELECT d1, sql_add(d1, cast(365*24*60*60 as interval second)) FROM TIMESTAMPTZ_TBL;

SELECT d1, sql_add(d1, cast(12 as interval month)) FROM TIMESTAMPTZ_TBL;
SELECT d1, sql_add(d1, cast(-18 as interval month)) FROM TIMESTAMPTZ_TBL;

SELECT d1, sql_sub(d1, 365*24*60*60.0) FROM TIMESTAMPTZ_TBL;
SELECT d1, sql_sub(d1, cast(365*24*60*60 as interval second)) FROM TIMESTAMPTZ_TBL;

SELECT d1, sql_sub(d1, cast(12 as interval month)) FROM TIMESTAMPTZ_TBL;
SELECT d1, sql_sub(d1, cast(-18 as interval month)) FROM TIMESTAMPTZ_TBL;

SELECT d1, sql_sub(d1, cast('2001-12-12 23:22:21' as timestamptz)) FROM TIMESTAMPTZ_TBL;
SELECT d1, sql_sub(cast('2001-12-12 23:22:21' as timestamptz), d1) FROM TIMESTAMPTZ_TBL;

DROP TABLE TIMESTAMPTZ_TBL;
