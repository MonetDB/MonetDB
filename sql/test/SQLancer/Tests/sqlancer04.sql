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
