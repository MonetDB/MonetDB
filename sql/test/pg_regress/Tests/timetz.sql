--
-- TIMETZ
--

CREATE TABLE TIMETZ_TBL (f1 time(2) with time zone);

INSERT INTO TIMETZ_TBL VALUES ('00:01 PDT');
INSERT INTO TIMETZ_TBL VALUES ('01:00 PDT');
INSERT INTO TIMETZ_TBL VALUES ('02:03 PDT');
INSERT INTO TIMETZ_TBL VALUES ('07:07 PST');
INSERT INTO TIMETZ_TBL VALUES ('08:08 EDT');
INSERT INTO TIMETZ_TBL VALUES ('11:59 PDT');
INSERT INTO TIMETZ_TBL VALUES ('12:00 PDT');
INSERT INTO TIMETZ_TBL VALUES ('12:01 PDT');
INSERT INTO TIMETZ_TBL VALUES ('23:59 PDT');
INSERT INTO TIMETZ_TBL VALUES ('11:59:59.99 PM PDT');
INSERT INTO TIMETZ_TBL VALUES (null);

SET TIME ZONE INTERVAL '+01:00' HOUR TO MINUTE;
SELECT f1 AS "Time TZ" FROM TIMETZ_TBL;

SELECT f1 AS "Three" FROM TIMETZ_TBL WHERE f1 < '05:06:07-07';

SELECT f1 AS "Seven" FROM TIMETZ_TBL WHERE f1 > '05:06:07-07';

SELECT f1 AS "None" FROM TIMETZ_TBL WHERE f1 < '00:00-07';

SELECT f1 AS "Ten" FROM TIMETZ_TBL WHERE f1 >= '00:00-07';

SET TIME ZONE INTERVAL '+04:30' HOUR TO MINUTE;
SELECT f1 AS "Time TZ" FROM TIMETZ_TBL ORDER BY 1;

SET TIME ZONE INTERVAL '+14:00' HOUR TO MINUTE;
SELECT f1 AS "Time TZ" FROM TIMETZ_TBL ORDER BY 1;

SET TIME ZONE INTERVAL '-07:00' HOUR TO MINUTE;
SELECT f1 AS "Time TZ" FROM TIMETZ_TBL ORDER BY 1;

SET TIME ZONE INTERVAL '+01:00' HOUR TO MINUTE;

--
-- TIME simple math
--
-- We now make a distinction between time and intervals,
-- and adding two times together makes no sense at all.
-- Leave in one query to show that it is rejected,
-- and do the rest of the testing in horology.sql
-- where we do mixed-type arithmetic. - thomas 2000-12-02

SELECT f1, f1 + time with time zone '00:01' AS "Illegal" FROM TIMETZ_TBL;


-- test MonetDB date functions
-- select distinct name, func, mod, language, type, schema_id from sys.functions where id in (select func_id from sys.args where number in (0, 1) and name in ('res_0', 'arg_1') and type = 'timetz') order by name, func, schema_id
-- select current_time();  -- retuns timestz
-- select localtime();     -- retuns time
-- select current_time;  -- retuns timetz
-- select localtime;     -- retuns time

SELECT f1, day(f1) FROM TIMETZ_TBL;
SELECT f1, "day"(f1) FROM TIMETZ_TBL;
SELECT f1, hour(f1) FROM TIMETZ_TBL;
SELECT f1, "hour"(f1) FROM TIMETZ_TBL;
SELECT f1, minute(f1) FROM TIMETZ_TBL;
SELECT f1, "minute"(f1) FROM TIMETZ_TBL;
SELECT f1, second(f1) FROM TIMETZ_TBL;
SELECT f1, "second"(f1) FROM TIMETZ_TBL;

SELECT f1, extract(day from f1) FROM TIMETZ_TBL;
SELECT f1, extract(hour from f1) FROM TIMETZ_TBL;
SELECT f1, extract(minute from f1) FROM TIMETZ_TBL;
SELECT f1, extract(second from f1) FROM TIMETZ_TBL;

SELECT f1, sql_add(f1, 12*60*60.0 + 66) FROM TIMETZ_TBL;
SELECT f1, sql_add(f1, cast(2*60*60 as interval second)) FROM TIMETZ_TBL;
SELECT f1, sql_add(f1, cast('2:44:59' as time)) FROM TIMETZ_TBL;
SELECT f1, sql_add(f1, cast('2:44:59 CET' as timetz)) FROM TIMETZ_TBL;

SELECT f1, sql_sub(f1, 12*60*60.0 + 66) FROM TIMETZ_TBL;
SELECT f1, sql_sub(f1, cast(2*60*60 as interval second)) FROM TIMETZ_TBL;

SELECT f1, sql_sub(f1, cast('23:22:21' as timetz)) FROM TIMETZ_TBL;
SELECT f1, sql_sub(cast('23:22:21' as timetz), f1) FROM TIMETZ_TBL;

-- next should give error
SELECT f1, month(f1) FROM TIMETZ_TBL;
SELECT f1, "month"(f1) FROM TIMETZ_TBL;
SELECT f1, year(f1) FROM TIMETZ_TBL;
SELECT f1, "year"(f1) FROM TIMETZ_TBL;
SELECT f1, extract(week from f1) FROM TIMETZ_TBL;
SELECT f1, extract(month from f1) FROM TIMETZ_TBL;
SELECT f1, extract(year from f1) FROM TIMETZ_TBL;
SELECT f1, "week"(f1) FROM TIMETZ_TBL;
SELECT f1, weekofyear(f1) FROM TIMETZ_TBL;
SELECT f1, dayofmonth(f1) FROM TIMETZ_TBL;
SELECT f1, dayofweek(f1) FROM TIMETZ_TBL;
SELECT f1, dayofyear(f1) FROM TIMETZ_TBL;

DROP TABLE TIMETZ_TBL;
