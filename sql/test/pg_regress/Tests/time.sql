--
-- TIME
--

CREATE TABLE TIME_TBL (f1 time(2));

INSERT INTO TIME_TBL VALUES ('00:00');
INSERT INTO TIME_TBL VALUES ('01:00');
-- as of 7.4, timezone spec should be accepted and ignored
INSERT INTO TIME_TBL VALUES ('02:03 PST');
INSERT INTO TIME_TBL VALUES ('11:59 EDT');
INSERT INTO TIME_TBL VALUES ('12:00');
INSERT INTO TIME_TBL VALUES ('12:01');
INSERT INTO TIME_TBL VALUES ('23:59');
INSERT INTO TIME_TBL VALUES ('11:59:59.99 AM');
INSERT INTO TIME_TBL VALUES ('11:59:59.99 PM');

SELECT f1 AS "Time" FROM TIME_TBL;

SELECT f1 AS "Three" FROM TIME_TBL WHERE f1 < '05:06:07';

SELECT f1 AS "Six" FROM TIME_TBL WHERE f1 > '05:06:07';

SELECT f1 AS "None" FROM TIME_TBL WHERE f1 < '00:00';

SELECT f1 AS "Nine" FROM TIME_TBL WHERE f1 >= '00:00';

--
-- TIME simple math
--
-- We now make a distinction between time and intervals,
-- and adding two times together makes no sense at all.
-- Leave in one query to show that it is rejected,
-- and do the rest of the testing in horology.sql
-- where we do mixed-type arithmetic. - thomas 2000-12-02

SELECT f1, f1 + time '00:01' AS "Illegal" FROM TIME_TBL;


INSERT INTO TIME_TBL VALUES (null);

-- test MonetDB date functions
-- select distinct name, func, mod, language, type, schema_id from sys.functions where id in (select func_id from sys.args where number in (0, 1) and name in ('res_0', 'arg_1') and type = 'time') order by name, func, schema_id
-- select current_time();  -- retuns timestz
-- select localtime();     -- retuns time
-- select current_time;  -- retuns timetz
-- select localtime;     -- retuns time

SELECT f1, day(f1) FROM TIME_TBL;
SELECT f1, "day"(f1) FROM TIME_TBL;
SELECT f1, hour(f1) FROM TIME_TBL;
SELECT f1, "hour"(f1) FROM TIME_TBL;
SELECT f1, minute(f1) FROM TIME_TBL;
SELECT f1, "minute"(f1) FROM TIME_TBL;
SELECT f1, second(f1) FROM TIME_TBL;
SELECT f1, "second"(f1) FROM TIME_TBL;

SELECT f1, extract(day from f1) FROM TIME_TBL;
SELECT f1, extract(hour from f1) FROM TIME_TBL;
SELECT f1, extract(minute from f1) FROM TIME_TBL;
SELECT f1, extract(second from f1) FROM TIME_TBL;

SELECT f1, sql_add(f1, 12*60*60.0 + 66) FROM TIME_TBL;
SELECT f1, sql_add(f1, cast(2*60*60 as interval second)) FROM TIME_TBL;
SELECT f1, sql_add(f1, cast('2:44:59' as time)) FROM TIME_TBL;
SELECT f1, sql_add(f1, cast('2:44:59 CET' as timetz)) FROM TIME_TBL;

SELECT f1, sql_sub(f1, 12*60*60.0 + 66) FROM TIME_TBL;
SELECT f1, sql_sub(f1, cast(2*60*60 as interval second)) FROM TIME_TBL;

SELECT f1, sql_sub(f1, cast('23:22:21' as time)) FROM TIME_TBL;
SELECT f1, sql_sub(cast('23:22:21' as time), f1) FROM TIME_TBL;

-- next should give error
SELECT f1, month(f1) FROM TIME_TBL;
SELECT f1, "month"(f1) FROM TIME_TBL;
SELECT f1, year(f1) FROM TIME_TBL;
SELECT f1, "year"(f1) FROM TIME_TBL;
SELECT f1, extract(week from f1) FROM TIME_TBL;
SELECT f1, extract(month from f1) FROM TIME_TBL;
SELECT f1, extract(year from f1) FROM TIME_TBL;
SELECT f1, week(f1) FROM TIME_TBL;
SELECT f1, weekofyear(f1) FROM TIME_TBL;
SELECT f1, dayofmonth(f1) FROM TIME_TBL;
SELECT f1, dayofweek(f1) FROM TIME_TBL;
SELECT f1, dayofyear(f1) FROM TIME_TBL;

DROP TABLE TIME_TBL;
