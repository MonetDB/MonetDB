--
-- DATE
--

CREATE TABLE DATE_TBL (f1 date);

INSERT INTO DATE_TBL VALUES ('1957-04-09');
INSERT INTO DATE_TBL VALUES ('1957-06-13');
INSERT INTO DATE_TBL VALUES ('1996-02-28');
INSERT INTO DATE_TBL VALUES ('1996-02-29');
INSERT INTO DATE_TBL VALUES ('1996-03-01');
INSERT INTO DATE_TBL VALUES ('1996-03-02');
INSERT INTO DATE_TBL VALUES ('1997-02-28');
INSERT INTO DATE_TBL VALUES ('1997-02-29');
INSERT INTO DATE_TBL VALUES ('1997-03-01');
INSERT INTO DATE_TBL VALUES ('1997-03-02');
INSERT INTO DATE_TBL VALUES ('2000-04-01');
INSERT INTO DATE_TBL VALUES ('2000-04-02');
INSERT INTO DATE_TBL VALUES ('2000-04-03');
INSERT INTO DATE_TBL VALUES ('2038-04-08');
INSERT INTO DATE_TBL VALUES ('2039-04-09');
INSERT INTO DATE_TBL VALUES ('2040-04-10');

SELECT f1 AS "Fifteen" FROM DATE_TBL;

SELECT f1 AS "Nine" FROM DATE_TBL WHERE f1 < '2000-01-01';

SELECT f1 AS "Three" FROM DATE_TBL
  WHERE f1 BETWEEN '2000-01-01' AND '2001-01-01';

INSERT INTO DATE_TBL VALUES (null);

-- test MonetDB date functions
-- select distinct name, func, mod, language, type, schema_id from sys.functions where id in (select func_id from sys.args where number in (0, 1) and name in ('res_0', 'arg_1') and type = 'date') order by name, func, schema_id
-- select curdate();
-- select current_date();
-- select current_date;

SELECT f1, day(f1) FROM DATE_TBL;
SELECT f1, "day"(f1) FROM DATE_TBL;
SELECT f1, month(f1) FROM DATE_TBL;
SELECT f1, "month"(f1) FROM DATE_TBL;
SELECT f1, year(f1) FROM DATE_TBL;
SELECT f1, "year"(f1) FROM DATE_TBL;

SELECT f1, extract(day from f1) FROM DATE_TBL;
SELECT f1, extract(week from f1) FROM DATE_TBL;
SELECT f1, extract(month from f1) FROM DATE_TBL;
SELECT f1, extract(quarter from f1) FROM DATE_TBL;
SELECT f1, extract(halfyear from f1) FROM DATE_TBL;
SELECT f1, extract(year from f1) FROM DATE_TBL;
SELECT f1, extract(century from f1) FROM DATE_TBL;
SELECT f1, extract(millenium from f1) FROM DATE_TBL;
SELECT f1, extract(epoch from f1) FROM DATE_TBL;

SELECT f1, "week"(f1) FROM DATE_TBL;
SELECT f1, weekofyear(f1) FROM DATE_TBL;

SELECT f1, dayofmonth(f1) FROM DATE_TBL;
SELECT f1, dayofweek(f1) FROM DATE_TBL;
SELECT f1, dayofyear(f1) FROM DATE_TBL;

SELECT f1, hour(f1) FROM DATE_TBL;
SELECT f1, "hour"(f1) FROM DATE_TBL;
SELECT f1, minute(f1) FROM DATE_TBL;
SELECT f1, "minute"(f1) FROM DATE_TBL;
SELECT f1, second(f1) FROM DATE_TBL;
SELECT f1, "second"(f1) FROM DATE_TBL;
SELECT f1, extract(hour from f1) FROM DATE_TBL;
SELECT f1, extract(minute from f1) FROM DATE_TBL;
SELECT f1, extract(second from f1) FROM DATE_TBL;
-- we need to cast the date into a timestamp
SELECT f1, cast(f1 as timestamp) FROM DATE_TBL;
SELECT f1, hour(cast(f1 as timestamp)) FROM DATE_TBL;
SELECT f1, "hour"(cast(f1 as timestamp)) FROM DATE_TBL;
SELECT f1, minute(cast(f1 as timestamp)) FROM DATE_TBL;
SELECT f1, "minute"(cast(f1 as timestamp)) FROM DATE_TBL;
SELECT f1, second(cast(f1 as timestamp)) FROM DATE_TBL;
SELECT f1, "second"(cast(f1 as timestamp)) FROM DATE_TBL;
SELECT f1, extract(hour from cast(f1 as timestamp)) FROM DATE_TBL;
SELECT f1, extract(minute from cast(f1 as timestamp)) FROM DATE_TBL;
SELECT f1, extract(second from cast(f1 as timestamp)) FROM DATE_TBL;

SELECT f1, sql_add(f1, 365*24*60*60.0) FROM DATE_TBL;
SELECT f1, sql_add(f1, cast(365*24*60*60 as interval second)) FROM DATE_TBL;

SELECT f1, sql_add(f1, cast(12 as interval month)) FROM DATE_TBL;
SELECT f1, sql_add(f1, cast(-18 as interval month)) FROM DATE_TBL;

SELECT f1, sql_sub(f1, 365*24*60*60.0) FROM DATE_TBL;
SELECT f1, sql_sub(f1, cast(365*24*60*60 as interval second)) FROM DATE_TBL;

SELECT f1, sql_sub(f1, cast(12 as interval month)) FROM DATE_TBL;
SELECT f1, sql_sub(f1, cast(-18 as interval month)) FROM DATE_TBL;

SELECT f1, sql_sub(f1, cast('2001-12-12' as date)) FROM DATE_TBL;
SELECT f1, sql_sub(cast('2001-12-12' as date), f1) FROM DATE_TBL;

--
-- Check all the documented input formats
--
/* SET datestyle TO iso; */ -- display results in ISO

/* SET datestyle TO ymd; */

SELECT date 'January 8, 1999';
SELECT date '1999-01-08';
SELECT date '1999-01-18';
SELECT date '1/8/1999';
SELECT date '1/18/1999';
SELECT date '18/1/1999';
SELECT date '01/02/03';
SELECT date '19990108';
SELECT date '990108';
SELECT date '1999.008';
SELECT date 'J2451187';
SELECT date 'January 8, 99 BC';

SELECT date '99-Jan-08';
SELECT date '1999-Jan-08';
SELECT date '08-Jan-99';
SELECT date '08-Jan-1999';
SELECT date 'Jan-08-99';
SELECT date 'Jan-08-1999';
SELECT date '99-08-Jan';
SELECT date '1999-08-Jan';

SELECT date '99 Jan 08';
SELECT date '1999 Jan 08';
SELECT date '08 Jan 99';
SELECT date '08 Jan 1999';
SELECT date 'Jan 08 99';
SELECT date 'Jan 08 1999';
SELECT date '99 08 Jan';
SELECT date '1999 08 Jan';

SELECT date '99-01-08';
SELECT date '1999-01-08';
SELECT date '08-01-99';
SELECT date '08-01-1999';
SELECT date '01-08-99';
SELECT date '01-08-1999';
SELECT date '99-08-01';
SELECT date '1999-08-01';

SELECT date '99 01 08';
SELECT date '1999 01 08';
SELECT date '08 01 99';
SELECT date '08 01 1999';
SELECT date '01 08 99';
SELECT date '01 08 1999';
SELECT date '99 08 01';
SELECT date '1999 08 01';

/* SET datestyle TO dmy; */

SELECT date 'January 8, 1999';
SELECT date '1999-01-08';
SELECT date '1999-01-18';
SELECT date '1/8/1999';
SELECT date '1/18/1999';
SELECT date '18/1/1999';
SELECT date '01/02/03';
SELECT date '19990108';
SELECT date '990108';
SELECT date '1999.008';
SELECT date 'J2451187';
SELECT date 'January 8, 99 BC';

SELECT date '99-Jan-08';
SELECT date '1999-Jan-08';
SELECT date '08-Jan-99';
SELECT date '08-Jan-1999';
SELECT date 'Jan-08-99';
SELECT date 'Jan-08-1999';
SELECT date '99-08-Jan';
SELECT date '1999-08-Jan';

SELECT date '99 Jan 08';
SELECT date '1999 Jan 08';
SELECT date '08 Jan 99';
SELECT date '08 Jan 1999';
SELECT date 'Jan 08 99';
SELECT date 'Jan 08 1999';
SELECT date '99 08 Jan';
SELECT date '1999 08 Jan';

SELECT date '99-01-08';
SELECT date '1999-01-08';
SELECT date '08-01-99';
SELECT date '08-01-1999';
SELECT date '01-08-99';
SELECT date '01-08-1999';
SELECT date '99-08-01';
SELECT date '1999-08-01';

SELECT date '99 01 08';
SELECT date '1999 01 08';
SELECT date '08 01 99';
SELECT date '08 01 1999';
SELECT date '01 08 99';
SELECT date '01 08 1999';
SELECT date '99 08 01';
SELECT date '1999 08 01';

/* SET datestyle TO mdy; */

SELECT date 'January 8, 1999';
SELECT date '1999-01-08';
SELECT date '1999-01-18';
SELECT date '1/8/1999';
SELECT date '1/18/1999';
SELECT date '18/1/1999';
SELECT date '01/02/03';
SELECT date '19990108';
SELECT date '990108';
SELECT date '1999.008';
SELECT date 'J2451187';
SELECT date 'January 8, 99 BC';

SELECT date '99-Jan-08';
SELECT date '1999-Jan-08';
SELECT date '08-Jan-99';
SELECT date '08-Jan-1999';
SELECT date 'Jan-08-99';
SELECT date 'Jan-08-1999';
SELECT date '99-08-Jan';
SELECT date '1999-08-Jan';

SELECT date '99 Jan 08';
SELECT date '1999 Jan 08';
SELECT date '08 Jan 99';
SELECT date '08 Jan 1999';
SELECT date 'Jan 08 99';
SELECT date 'Jan 08 1999';
SELECT date '99 08 Jan';
SELECT date '1999 08 Jan';

SELECT date '99-01-08';
SELECT date '1999-01-08';
SELECT date '08-01-99';
SELECT date '08-01-1999';
SELECT date '01-08-99';
SELECT date '01-08-1999';
SELECT date '99-08-01';
SELECT date '1999-08-01';

SELECT date '99 01 08';
SELECT date '1999 01 08';
SELECT date '08 01 99';
SELECT date '08 01 1999';
SELECT date '01 08 99';
SELECT date '01 08 1999';
SELECT date '99 08 01';
SELECT date '1999 08 01';

/* RESET datestyle; */

--
-- Simple math
-- Leave most of it for the horology tests
--

SELECT f1 - date '2000-01-01' AS "Days From 2K" FROM DATE_TBL;

SELECT f1 - date 'epoch' AS "Days From Epoch" FROM DATE_TBL;

-- SELECT current_date AS "today";
-- SELECT sql_sub(current_date, 24*60*60.0) AS "yesterday";
-- SELECT sql_add(current_date, 24*60*60.0) AS "tomorrow";

-- SELECT date 'yesterday' - date 'today' AS "One day";
SELECT sql_sub(current_date, 24*60*60.0) - current_date AS "One day";

-- SELECT date 'today' - date 'tomorrow' AS "One day";
SELECT current_date - sql_add(current_date, 24*60*60.0) AS "One day";

-- SELECT date 'yesterday' - date 'tomorrow' AS "Two days";
SELECT sql_sub(current_date, 24*60*60.0) - sql_add(current_date, 24*60*60.0) AS "Two days";

-- SELECT date 'tomorrow' - date 'today' AS "One day";
SELECT sql_add(current_date, 24*60*60.0) - current_date AS "One day";

-- SELECT date 'today' - date 'yesterday' AS "One day";
SELECT current_date - sql_sub(current_date, 24*60*60.0) AS "One day";

-- SELECT date 'tomorrow' - date 'yesterday' AS "Two days";
SELECT sql_add(current_date, 24*60*60.0) - sql_sub(current_date, 24*60*60.0) AS "Two days";

--
-- test extract!
--
-- century
--
SELECT EXTRACT(CENTURY FROM DATE '0101-12-31 BC'); -- -2
SELECT EXTRACT(CENTURY FROM DATE '0100-12-31 BC'); -- -1
SELECT EXTRACT(CENTURY FROM DATE '0001-12-31 BC'); -- -1
SELECT EXTRACT(CENTURY FROM DATE '0001-01-01');    --  1
SELECT EXTRACT(CENTURY FROM DATE '0001-01-01 AD'); --  1
SELECT EXTRACT(CENTURY FROM DATE '1900-12-31');    -- 19
SELECT EXTRACT(CENTURY FROM DATE '1901-01-01');    -- 20
SELECT EXTRACT(CENTURY FROM DATE '2000-12-31');    -- 20
SELECT EXTRACT(CENTURY FROM DATE '2001-01-01');    -- 21
SELECT EXTRACT(CENTURY FROM CURRENT_DATE)>=21 AS True;     -- true
--
-- millennium
--
SELECT EXTRACT(MILLENNIUM FROM DATE '0001-12-31 BC'); -- -1
SELECT EXTRACT(MILLENNIUM FROM DATE '0001-01-01 AD'); --  1
SELECT EXTRACT(MILLENNIUM FROM DATE '1000-12-31');    --  1
SELECT EXTRACT(MILLENNIUM FROM DATE '1001-01-01');    --  2
SELECT EXTRACT(MILLENNIUM FROM DATE '2000-12-31');    --  2
SELECT EXTRACT(MILLENNIUM FROM DATE '2001-01-01');    --  3
-- next test to be fixed on the turn of the next millennium;-)
SELECT EXTRACT(MILLENNIUM FROM CURRENT_DATE);         --  3
--
-- decade
--
SELECT EXTRACT(DECADE FROM DATE '1994-12-25');    -- 199
SELECT EXTRACT(DECADE FROM DATE '0010-01-01');    --   1
SELECT EXTRACT(DECADE FROM DATE '0009-12-31');    --   0
SELECT EXTRACT(DECADE FROM DATE '0001-01-01 BC'); --   0
SELECT EXTRACT(DECADE FROM DATE '0002-12-31 BC'); --  -1
SELECT EXTRACT(DECADE FROM DATE '0011-01-01 BC'); --  -1
SELECT EXTRACT(DECADE FROM DATE '0012-12-31 BC'); --  -2
--
-- some other types:
--
-- on a timestamp.
SELECT EXTRACT(CENTURY FROM NOW())>=21 AS True;       -- true
SELECT EXTRACT(CENTURY FROM TIMESTAMP '1970-03-20 04:30:00.00000'); -- 20
-- on an interval
SELECT EXTRACT(CENTURY FROM INTERVAL '100 y');  -- 1
SELECT EXTRACT(CENTURY FROM INTERVAL '99 y');   -- 0
SELECT EXTRACT(CENTURY FROM INTERVAL '-99 y');  -- 0
SELECT EXTRACT(CENTURY FROM INTERVAL '-100 y'); -- -1
--
-- test trunc function!
--
SELECT DATE_TRUNC('MILLENNIUM', TIMESTAMP '1970-03-20 04:30:00.00000'); -- 1001
SELECT DATE_TRUNC('MILLENNIUM', DATE '1970-03-20'); -- 1001-01-01
SELECT DATE_TRUNC('CENTURY', TIMESTAMP '1970-03-20 04:30:00.00000'); -- 1901
SELECT DATE_TRUNC('CENTURY', DATE '1970-03-20'); -- 1901
SELECT DATE_TRUNC('CENTURY', DATE '2004-08-10'); -- 2001-01-01
SELECT DATE_TRUNC('CENTURY', DATE '0002-02-04'); -- 0001-01-01
SELECT DATE_TRUNC('CENTURY', DATE '0055-08-10 BC'); -- 0100-01-01 BC
SELECT DATE_TRUNC('DECADE', DATE '1993-12-25'); -- 1990-01-01
SELECT DATE_TRUNC('DECADE', DATE '0004-12-25'); -- 0001-01-01 BC
SELECT DATE_TRUNC('DECADE', DATE '0002-12-31 BC'); -- 0011-01-01 BC

DROP TABLE DATE_TBL;
