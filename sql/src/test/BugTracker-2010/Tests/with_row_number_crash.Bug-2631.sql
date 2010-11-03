-- we don't care about content, but we do care about length
-- create some temporary tables and views to work with
create table _t2631 as select * from _tables limit 20 with data;
create table _tt2631 as select * from tmp._tables with no data;
create view t2631 as SELECT * FROM (SELECT p.*, 0 AS "temporary" FROM "sys"."_t2631" AS p UNION ALL SELECT t.*, 1 AS "temporary" FROM "_tt2631" AS t) AS t2631 where t2631.type < 2;

WITH t2 (i) AS (SELECT ROW_NUMBER () OVER (ORDER BY id ASC) AS i FROM t2631) select i from t2;

WITH t1 (i) AS (SELECT ROW_NUMBER () OVER (ORDER BY id ASC) AS i FROM _t2631) select i from t1;

drop view t2631;
drop table _tt2631;
drop table _t2631;
