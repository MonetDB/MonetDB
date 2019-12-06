CREATE TABLE INTERVAL_TBL (f1 interval second);
INSERT INTO INTERVAL_TBL (f1) VALUES (1.123);
INSERT INTO INTERVAL_TBL (f1) VALUES (2.123);
INSERT INTO INTERVAL_TBL (f1) VALUES (3.123);
INSERT INTO INTERVAL_TBL (f1) VALUES (4.123);
SELECT f1, cast(f1 as int), cast(f1 as dec(8,3)), cast(f1 as real) FROM INTERVAL_TBL;
SELECT count(f1), sum(f1), cast(sum(f1) as int), cast(sum(f1) as dec(8,3)), cast(sum(f1) as real) FROM INTERVAL_TBL;
DROP TABLE INTERVAL_TBL;

CREATE TABLE INTERVAL_TBL (f1 interval month);
INSERT INTO INTERVAL_TBL (f1) VALUES (1);
INSERT INTO INTERVAL_TBL (f1) VALUES (2);
INSERT INTO INTERVAL_TBL (f1) VALUES (3);
INSERT INTO INTERVAL_TBL (f1) VALUES (4);
SELECT f1, cast(f1 as int) FROM INTERVAL_TBL;
SELECT f1, cast(f1 as dec(3,0)) FROM INTERVAL_TBL;  -- returns error: types month_interval(3,0) and decimal(3,0) are not equal for column 'f1'
SELECT f1, cast(f1 as real) FROM INTERVAL_TBL;  --  returns error: types month_interval(3,0) and real(24,0) are not equal for column 'f1'
SELECT count(f1), sum(f1), cast(sum(f1) as int), cast(sum(f1) as dec(8,3)), cast(sum(f1) as real) FROM INTERVAL_TBL;
DROP TABLE INTERVAL_TBL;

