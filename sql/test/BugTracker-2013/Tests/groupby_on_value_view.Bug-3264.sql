start transaction;
CREATE VIEW mmtest_v AS SELECT date'2012-01-01' as period, 1 as quantity;
SELECT extract(month from period) as m, count(1) as cnt FROM mmtest_v GROUP BY m;
rollback;

