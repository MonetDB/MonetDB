create table analytics (aa int, bb int, cc bigint);
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);

start transaction;

select percent_rank() over (partition by aa) from analytics;
select percent_rank() over (partition by aa order by aa asc) from analytics;
select percent_rank() over (partition by aa order by aa desc) from analytics;
select percent_rank() over (order by aa) from analytics;
select percent_rank() over (order by aa desc) from analytics;

select percent_rank() over (partition by bb) from analytics;
select percent_rank() over (partition by bb order by bb asc) from analytics;
select percent_rank() over (partition by bb order by bb desc) from analytics;
select percent_rank() over (order by bb) from analytics;
select percent_rank() over (order by bb desc) from analytics;

select cume_dist() over (partition by aa) from analytics;
select cume_dist() over (partition by aa order by aa asc) from analytics;
select cume_dist() over (partition by aa order by aa desc) from analytics;
select cume_dist() over (order by aa) from analytics;
select cume_dist() over (order by aa desc) from analytics;

select cume_dist() over (partition by bb) from analytics;
select cume_dist() over (partition by bb order by bb asc) from analytics;
select cume_dist() over (partition by bb order by bb desc) from analytics;
select cume_dist() over (order by bb) from analytics;
select cume_dist() over (order by bb desc) from analytics;

select ntile(1) over (partition by aa) from analytics;
select ntile(1) over (partition by aa order by aa asc) from analytics;
select ntile(1) over (partition by aa order by aa desc) from analytics;
select ntile(1) over (order by aa) from analytics;
select ntile(1) over (order by aa desc) from analytics;

select ntile(3) over (partition by bb) from analytics;
select ntile(3) over (partition by bb order by bb asc) from analytics;
select ntile(3) over (partition by bb order by bb desc) from analytics;
select ntile(3) over (order by bb) from analytics;
select ntile(3) over (order by bb desc) from analytics;

select ntile(10) over (partition by aa) from analytics;
select ntile(10) over (partition by aa order by aa asc) from analytics;
select ntile(10) over (partition by aa order by aa desc) from analytics;
select ntile(10) over (order by aa) from analytics;
select ntile(10) over (order by aa desc) from analytics;

select ntile(aa) over (partition by aa) from analytics;
select ntile(aa) over (partition by aa order by aa asc) from analytics;
select ntile(aa) over (partition by aa order by aa desc) from analytics;
select ntile(aa) over (order by aa) from analytics;
select ntile(aa) over (order by aa desc) from analytics;

select ntile(aa) over (partition by bb) from analytics;
select ntile(aa) over (partition by bb order by bb asc) from analytics;
select ntile(aa) over (partition by bb order by bb desc) from analytics;
select ntile(aa) over (order by bb) from analytics;
select ntile(aa) over (order by bb desc) from analytics;

select ntile(bb) over (partition by aa) from analytics;
select ntile(bb) over (partition by aa order by aa asc) from analytics;
select ntile(bb) over (partition by aa order by aa desc) from analytics;
select ntile(bb) over (order by aa) from analytics;
select ntile(bb) over (order by aa desc) from analytics;

select ntile(bb) over (partition by bb) from analytics;
select ntile(bb) over (partition by bb order by bb asc) from analytics;
select ntile(bb) over (partition by bb order by bb desc) from analytics;
select ntile(bb) over (order by bb) from analytics;
select ntile(bb) over (order by bb desc) from analytics;

select first_value(aa) over (partition by aa) from analytics;
select first_value(aa) over (partition by aa order by aa asc) from analytics;
select first_value(aa) over (partition by aa order by aa desc) from analytics;
select first_value(aa) over (order by aa) from analytics;
select first_value(aa) over (order by aa desc) from analytics;

select first_value(aa) over (partition by bb) from analytics;
select first_value(aa) over (partition by bb order by bb asc) from analytics;
select first_value(aa) over (partition by bb order by bb desc) from analytics;
select first_value(aa) over (order by bb) from analytics;
select first_value(aa) over (order by bb desc) from analytics;

select nth_value(aa, 1) over (partition by bb) from analytics;
select nth_value(aa, 1) over (partition by bb order by bb asc) from analytics;
select nth_value(aa, 1) over (partition by bb order by bb desc) from analytics;
select nth_value(aa, 1) over (order by bb) from analytics;
select nth_value(aa, 1) over (order by bb desc) from analytics;

select last_value(bb) over (partition by aa) from analytics;
select last_value(bb) over (partition by aa order by aa asc) from analytics;
select last_value(bb) over (partition by aa order by aa desc) from analytics;
select last_value(bb) over (order by aa) from analytics;
select last_value(bb) over (order by aa desc) from analytics;

select last_value(bb) over (partition by bb) from analytics;
select last_value(bb) over (partition by bb order by bb asc) from analytics;
select last_value(bb) over (partition by bb order by bb desc) from analytics;
select last_value(bb) over (order by bb) from analytics;
select last_value(bb) over (order by bb desc) from analytics;

select nth_value(bb, 3) over (partition by bb) from analytics;
select nth_value(bb, 3) over (partition by bb order by bb asc) from analytics;
select nth_value(bb, 3) over (partition by bb order by bb desc) from analytics;
select nth_value(bb, 3) over (order by bb) from analytics;
select nth_value(bb, 3) over (order by bb desc) from analytics;

select lag(bb) over (partition by aa) from analytics;
select lag(bb) over (partition by aa order by aa asc) from analytics;
select lag(bb) over (partition by aa order by aa desc) from analytics;
select lag(bb) over (order by aa) from analytics;
select lag(bb) over (order by aa desc) from analytics;

select lead(bb) over (partition by bb) from analytics;
select lead(bb) over (partition by bb order by bb asc) from analytics;
select lead(bb) over (partition by bb order by bb desc) from analytics;
select lead(bb) over (order by bb) from analytics;
select lead(bb) over (order by bb desc) from analytics;

select row_number() over () from analytics;
select rank() over () from analytics;
select dense_rank() over () from analytics;
select percent_rank() over () from analytics;
select cume_dist() over () from analytics;
select ntile(1) over () from analytics;
select ntile(2) over () from analytics;
select ntile(5) over () from analytics;
select ntile(11) over () from analytics;
select ntile(100) over () from analytics;

select first_value(aa) over () from analytics;
select first_value(bb) over () from analytics;
select last_value(aa) over () from analytics;
select last_value(bb) over () from analytics;
select nth_value(aa, 2) over () from analytics;
select nth_value(bb, 1) over () from analytics;
select nth_value(bb, 1000) over () from analytics;
select lag(aa) over () from analytics;
select lead(bb) over () from analytics;
select lag(aa) over () from analytics;
select lead(bb) over () from analytics;

select ntile(null) over () from analytics;
select first_value(null) over () from analytics;
select last_value(null) over () from analytics;
select nth_value(null, 1) over () from analytics;
select nth_value(aa, null) over () from analytics;
select nth_value(1, null) over () from analytics;
select nth_value(null, null) over () from analytics;
select lag(null) over () from analytics;
select lag(null, null) over () from analytics;
select lag(null, null, null) over () from analytics;
select lead(null) over () from analytics;
select lead(null, null) over () from analytics;
select lead(null, null, null) over () from analytics;

create table stressme (aa varchar(64), bb int);
insert into stressme values ('one', 1), ('another', 1), ('stress', 1), (NULL, 2), ('ok', 2), ('check', 3), ('me', 3), ('please', 3), (NULL, 4);

select first_value(aa) over (partition by bb) from stressme;
select first_value(aa) over (partition by bb order by bb asc) from stressme;
select first_value(aa) over (partition by bb order by bb desc) from stressme;
select first_value(aa) over (order by bb) from stressme;
select first_value(aa) over (order by bb desc) from stressme;

select first_value(bb) over (partition by aa) from stressme;
select first_value(bb) over (partition by aa order by aa asc) from stressme;
select first_value(bb) over (partition by aa order by aa desc) from stressme;
select first_value(bb) over (order by aa) from stressme;
select first_value(bb) over (order by aa desc) from stressme;

select last_value(aa) over (partition by bb) from stressme;
select last_value(aa) over (partition by bb order by bb asc) from stressme;
select last_value(aa) over (partition by bb order by bb desc) from stressme;
select last_value(aa) over (order by bb) from stressme;
select last_value(aa) over (order by bb desc) from stressme;

select last_value(bb) over (partition by aa) from stressme;
select last_value(bb) over (partition by aa order by aa asc) from stressme;
select last_value(bb) over (partition by aa order by aa desc) from stressme;
select last_value(bb) over (order by aa) from stressme;
select last_value(bb) over (order by aa desc) from stressme;

select nth_value(aa, 1) over (partition by bb) from stressme;
select nth_value(aa, 1) over (partition by bb order by bb asc) from stressme;
select nth_value(aa, 1) over (partition by bb order by bb desc) from stressme;
select nth_value(aa, 1) over (order by bb) from stressme;
select nth_value(aa, 1) over (order by bb desc) from stressme;

select nth_value(aa, 5) over (partition by bb) from stressme;
select nth_value(aa, 5) over (partition by bb order by bb asc) from stressme;
select nth_value(aa, 5) over (partition by bb order by bb desc) from stressme;
select nth_value(aa, 5) over (order by bb) from stressme;
select nth_value(aa, 5) over (order by bb desc) from stressme;

select nth_value(bb, 1) over (partition by aa) from stressme;
select nth_value(bb, 1) over (partition by aa order by aa asc) from stressme;
select nth_value(bb, 1) over (partition by aa order by aa desc) from stressme;
select nth_value(bb, 1) over (order by aa) from stressme;
select nth_value(bb, 1) over (order by aa desc) from stressme;

select nth_value(bb, 5) over (partition by aa) from stressme;
select nth_value(bb, 5) over (partition by aa order by aa asc) from stressme;
select nth_value(bb, 5) over (partition by aa order by aa desc) from stressme;
select nth_value(bb, 5) over (order by aa) from stressme;
select nth_value(bb, 5) over (order by aa desc) from stressme;

select lag(aa) over (partition by bb) from stressme;
select lag(aa) over (partition by bb order by bb asc) from stressme;
select lag(aa) over (partition by bb order by bb desc) from stressme;
select lag(aa) over (order by bb) from stressme;
select lag(aa) over (order by bb desc) from stressme;

select lead(aa) over (partition by bb) from stressme;
select lead(aa) over (partition by bb order by bb asc) from stressme;
select lead(aa) over (partition by bb order by bb desc) from stressme;
select lead(aa) over (order by bb) from stressme;
select lead(aa) over (order by bb desc) from stressme;

select lag(bb) over (partition by aa) from stressme;
select lag(bb) over (partition by aa order by aa asc) from stressme;
select lag(bb) over (partition by aa order by aa desc) from stressme;
select lag(bb) over (order by aa) from stressme;
select lag(bb) over (order by aa desc) from stressme;

select lead(bb) over (partition by aa) from stressme;
select lead(bb) over (partition by aa order by aa asc) from stressme;
select lead(bb) over (partition by aa order by aa desc) from stressme;
select lead(bb) over (order by aa) from stressme;
select lead(bb) over (order by aa desc) from stressme;

select aa, bb, lead(aa, 2, 100) over (partition by bb), lead(aa, 1, '100') over (partition by bb) from analytics;

select nth_value(aa, aa) over () from analytics;
select nth_value(1, aa) over () from analytics;

create table t1 (col1 int, col2 int);
insert into t1 values (8481, 0), (8489, 0), (8489, 1), (8498, 0), (8498, 1), (8498, 2), (8507, 0), (8507, 1), (8507, 2);
select col1, col2, lag(col2) over (partition by col1 ORDER BY col2), lag(col2, 2) over (partition by col1 ORDER BY col2), lag(col2, 3) over (partition by col1 ORDER BY col2) from t1;

select lag(col2, -1) over (partition by col1 ORDER BY col2), lag(col2, 1) over (partition by col1 ORDER BY col2), lag(col2, 2) over (partition by col1 ORDER BY col2) from t1;
select lead(col2, -1) over (partition by col1 ORDER BY col2), lead(col2, 1) over (partition by col1 ORDER BY col2), lead(col2, 2) over (partition by col1 ORDER BY col2) from t1;

CREATE TABLE "sys"."test1" (
	"name"       VARCHAR(100),
	"points"     DOUBLE,
	"start_time" TIMESTAMP
);
COPY 8 RECORDS INTO "sys"."test1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
"Hello"	20	"2017-12-01 00:00:00.000000"
"Hello"	40	"2017-12-01 01:00:00.000000"
"Hello"	60	"2017-12-01 00:00:00.000000"
"World"	10	"2017-12-01 00:00:00.000000"
"World"	50	"2017-12-01 01:00:00.000000"
"World"	90	"2017-12-01 00:00:00.000000"
"World"	11	"2017-12-02 01:02:00.000000"
"World"	15	"2017-12-02 02:02:00.000000"

SELECT CAST(t0.start_time AS DATE) AS start_time,
FIRST_VALUE(t0.points) OVER (PARTITION BY CAST(t0.start_time AS DATE) ORDER BY t0.start_time) AS first_point,
LAST_VALUE(t0.points) OVER (PARTITION BY CAST(t0.start_time AS DATE) ORDER BY t0.start_time) AS last_point
FROM test1 t0
WHERE (t0.start_time >= '2017/12/01 00:00:00' AND t0.start_time <= '2017/12/02 00:00:00');

SELECT DISTINCT CAST(t0.start_time AS DATE) AS start_time,
FIRST_VALUE(t0.points) OVER (PARTITION BY CAST(t0.start_time AS DATE) ORDER BY t0.start_time) AS first_point,
LAST_VALUE(t0.points) OVER (PARTITION BY CAST(t0.start_time AS DATE) ORDER BY t0.start_time) AS last_point
FROM test1 t0
WHERE (t0.start_time >= '2017/12/01 00:00:00' AND t0.start_time <= '2017/12/02 00:00:00');

CREATE TABLE "sys"."test2" (
	"name"       VARCHAR(100),
	"points"     INT,
	"start_time" TIMESTAMP
);

COPY 8 RECORDS INTO "sys"."test2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
"Ashish"	20	"2017-12-01 00:00:00.000000"
"Ashish"	40	"2017-12-01 01:00:00.000000"
"Ashish"	60	"2017-12-01 00:00:00.000000"
"Prashant"	10	"2017-12-01 00:00:00.000000"
"Prashant"	50	"2017-12-01 01:00:00.000000"
"Prashant"	90	"2017-12-01 00:00:00.000000"
"Prashant"	11	"2017-12-02 01:02:00.000000"
"Prashant"	15	"2017-12-02 02:02:00.000000"

SELECT distinct "name" ,
first_value("points") over (partition by cast("start_time" as date) order by cast("start_time" as date))
FROM test2
order by "name";

SELECT distinct "name" ,
first_value("points") over (partition by cast("start_time" as date), "name" order by cast("start_time" as date))
FROM test2
order by "name";

rollback;

select ntile(1) from analytics; --error, ntile requires an OVER clause
select ntile(distinct aa) over () from analytics; --error
select nth_value(distinct aa, bb) over () from analytics; --error

select lead(aa, 34, 1000000000000) over (partition by bb) from analytics; --error

drop table analytics;
