CREATE TABLE INTERVAL_TBL (f1 interval second);
INSERT INTO INTERVAL_TBL (f1) VALUES (interval '1' second), (interval '2' second), (interval '3' second), (interval '4' second);
SELECT count(f1), sum(f1), avg(f1), median(f1), max(f1), min(f1) FROM INTERVAL_TBL;

CREATE TABLE INTERVAL_TBL2 (f1 interval month);
INSERT INTO INTERVAL_TBL2 (f1) VALUES (interval '1' month), (interval '2' month), (interval '3' month), (interval '4' month);
SELECT count(f1), sum(f1), avg(f1), median(f1), max(f1), min(f1) FROM INTERVAL_TBL2;

SELECT median_avg(f1) from INTERVAL_TBL;
SELECT median_avg(f1) from INTERVAL_TBL2;

DROP TABLE INTERVAL_TBL;
DROP TABLE INTERVAL_TBL2;
