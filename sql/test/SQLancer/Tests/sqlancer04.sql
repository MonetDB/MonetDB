START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" INTERVAL MONTH NOT NULL);
COPY 3 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
150184010
332127116
2136089006

-- this query has to run twice to trigger the assertion error
SELECT SUM(ALL 0.12830007105673624234754015560611151158) FROM t0 WHERE ((sql_min(t0.c0, t0.c0))<>(COALESCE (t0.c0, sql_min(abs(t0.c0), nullif(t0.c0, t0.c0))))) GROUP BY 0.1 
HAVING MIN(ALL ((r'946496923')LIKE(CAST(nullif(0.5, 0.03) AS STRING(538))))) 
UNION ALL SELECT SUM(ALL 0.12830007105673624234754015560611151158) FROM t0 WHERE ((sql_min(t0.c0, t0.c0))<>(COALESCE (t0.c0, sql_min(abs(t0.c0), nullif(t0.c0, t0.c0))))) GROUP BY 0.1 
HAVING NOT (MIN(ALL ((r'946496923')LIKE(CAST(nullif(0.5, 0.03) AS STRING(538)))))) 
UNION ALL SELECT ALL SUM(ALL 0.12830007105673624234754015560611151158) FROM t0 WHERE ((sql_min(t0.c0, t0.c0))<>(COALESCE (t0.c0, sql_min(abs(t0.c0), nullif(t0.c0, t0.c0))))) GROUP BY 0.1 
HAVING (MIN(ALL ((r'946496923')LIKE(CAST(nullif(0.5, 0.03) AS STRING(538)))))) IS NULL;

SELECT SUM(ALL 0.12830007105673624234754015560611151158) FROM t0 WHERE ((sql_min(t0.c0, t0.c0))<>(COALESCE (t0.c0, sql_min(abs(t0.c0), nullif(t0.c0, t0.c0))))) GROUP BY 0.1 
HAVING MIN(ALL ((r'946496923')LIKE(CAST(nullif(0.5, 0.03) AS STRING(538))))) 
UNION ALL SELECT SUM(ALL 0.12830007105673624234754015560611151158) FROM t0 WHERE ((sql_min(t0.c0, t0.c0))<>(COALESCE (t0.c0, sql_min(abs(t0.c0), nullif(t0.c0, t0.c0))))) GROUP BY 0.1 
HAVING NOT (MIN(ALL ((r'946496923')LIKE(CAST(nullif(0.5, 0.03) AS STRING(538)))))) 
UNION ALL SELECT ALL SUM(ALL 0.12830007105673624234754015560611151158) FROM t0 WHERE ((sql_min(t0.c0, t0.c0))<>(COALESCE (t0.c0, sql_min(abs(t0.c0), nullif(t0.c0, t0.c0))))) GROUP BY 0.1 
HAVING (MIN(ALL ((r'946496923')LIKE(CAST(nullif(0.5, 0.03) AS STRING(538)))))) IS NULL;
ROLLBACK;

SELECT covar_samp(1, - (COALESCE(1, 2)||5)); --error on default, covar_samp between integer and string not possible

START TRANSACTION;
CREATE TABLE "sys"."t0"("c0" DATE, "c1" DATE, "c2" INTERVAL SECOND NOT NULL,"c3" TIME NOT NULL);

select case covar_samp(all - (coalesce (cast(-5 as int), coalesce (((cast(0.4 as int))-(coalesce (5, 5))), + 
(case -3 when 0.5 then 5 else 3 end)))), - (- (((coalesce (cast(0.2 as int), 
coalesce (5, 3, -747176383), ((6)%(3)), ((-2)<<(3))))||(-5))))) when case t0.c0 when 
case coalesce (coalesce (0.2, 0.3, 0.7), 0.5, 
cast(t0.c2 as decimal)) when cast(interval '-4' month as interval second) then cast(case 0.3 when t0.c1 then 0.6 
when interval '-3' month then 0.3 end as interval second) end then coalesce (abs(0.6), 
cast(lower(r'') as decimal), 0.4) end then abs(0.5) when abs(-1.4) 
then coalesce (sql_min(case t0.c0 when t0.c1 then 0.5 else 0.4 end, 
case 0.5 when t0.c2 then 0.6 when 0.5 then 0.2 else 0.7 end), abs(coalesce (0.3, 0.5, 0.3))) 
else coalesce (case coalesce (dayofmonth(timestamp '1970-01-15 10:08:18'), coalesce (-5, 3, 5)) when coalesce (cast(timestamp '1970-01-18 00:15:34' as double), 
cast(t0.c3 as double)) then case least(t0.c1, t0.c1) when case t0.c2 when interval '5' month then r'*pf6/+}öq壚,j2\302\205K]sNKk,_%Tu' when 1016331084 then r'0.4'
else r'*' end then 0.8 end when sql_min(t0.c3, t0.c3) then coalesce (cast(t0.c2 as decimal), "second"(t0.c3), cast(t0.c2 as decimal),
0.9) when coalesce (cast(t0.c1 as double), 0.2) then 0.0 else 0.0 end, 0.2) end from t0 where (interval '6' month)
is not null group by cast(dayofmonth(t0.c0) as string(679)), 0.2; --error, on Jun2020 t0.c0 is not aggregated, on default
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" INTERVAL SECOND NOT NULL);
SELECT CASE '5'^3 WHEN COUNT(TRUE) THEN 1 END FROM t0 GROUP BY 2 IN ((CAST(INTERVAL '-2' SECOND AS INT))%2); --error on default: types sec_interval(13,0) and int(32,0) are not equal
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" CHARACTER LARGE OBJECT NOT NULL,"c1" BIGINT NOT NULL,CONSTRAINT "t0_c1_pkey" PRIMARY KEY ("c1"));
CREATE TABLE "sys"."t1" ("c0" CHARACTER LARGE OBJECT,"c1" BIGINT);
COPY 3 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
"-216073164"	NULL
"-2044926527"	NULL
NULL	1

SELECT 1 FROM t1 LEFT OUTER JOIN t0 ON TRUE 
LEFT OUTER JOIN (SELECT 1 FROM t0) AS sub0 ON TRUE 
WHERE (TIME '00:25:07') IN (TIME '07:29:34', CASE 2 WHEN 2 THEN TIME '17:23:46' ELSE TIME '05:14:30' END);
	-- empty

SELECT 1 FROM t1 LEFT OUTER JOIN t0 ON TRUE 
LEFT OUTER JOIN (SELECT 1 FROM t0) AS sub0 ON TRUE 
WHERE t0.c0 <= t0.c0 AND (TIME '00:25:07') IN (TIME '07:29:34', CASE 2 WHEN 2 THEN TIME '17:23:46' ELSE TIME '05:14:30' END);
	-- empty

SELECT ALL COUNT(TIME '06:32:50') FROM t1 LEFT OUTER JOIN t0 ON TRUE 
LEFT OUTER JOIN (SELECT t1.c1, t0.c1, 0.43 FROM t0, t1) AS sub0 ON TRUE 
WHERE ((NOT ((TIME '00:25:07') IN (TIME '07:29:34', TIME '05:21:58', CASE 0.54 WHEN 0.65 THEN TIME '17:23:46' ELSE TIME '05:14:30' END)))) 
GROUP BY TIMESTAMP '1969-12-08 01:47:58';
ROLLBACK;

CREATE TABLE t0(c0 TIME, UNIQUE(c0));
CREATE TABLE t1(c0 CHAR(222), FOREIGN KEY (c0) REFERENCES t0(c0) MATCH FULL, PRIMARY KEY(c0)); --error, foreign key from char to time not allowed
CREATE TABLE t1(c0 int, FOREIGN KEY (c0) REFERENCES t0(c0) MATCH FULL, PRIMARY KEY(c0)); --error, foreign key from int to time not allowed
DROP TABLE t0;

CREATE TABLE t0(c0 INTERVAL SECOND, UNIQUE(c0));
CREATE TABLE t1(c2 BLOB, FOREIGN KEY (c2) REFERENCES t0(c0) MATCH FULL, PRIMARY KEY(c2)); --error, foreign key from blob to interval second not allowed
CREATE TABLE t1(c2 TIME, FOREIGN KEY (c2) REFERENCES t0(c0) MATCH FULL, PRIMARY KEY(c2)); --error, foreign key from time to interval second not allowed
DROP TABLE t0;
