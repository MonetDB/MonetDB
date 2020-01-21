CREATE TABLE INTERVAL_TBL (f1 interval second);
INSERT INTO INTERVAL_TBL (f1) VALUES (1), (2), (3), (4);
SELECT count(f1), cast(sum(f1) as bigint), avg(f1), median(f1), max(f1), min(f1) FROM INTERVAL_TBL;

CREATE TABLE INTERVAL_TBL2 (f1 interval month);
INSERT INTO INTERVAL_TBL2 (f1) VALUES (1), (2), (3), (4);
SELECT count(f1), cast(sum(f1) as bigint), avg(f1), median(f1), max(f1), min(f1) FROM INTERVAL_TBL2;

SELECT median_avg(f1) from INTERVAL_TBL; --error, median_avg is not available yet for sec_interval
SELECT median_avg(f1) from INTERVAL_TBL2;

DROP TABLE INTERVAL_TBL;
DROP TABLE INTERVAL_TBL2;
