CREATE TABLE INTERVAL_TBL (f1 interval second);
INSERT INTO INTERVAL_TBL (f1) VALUES (interval '1' second);
INSERT INTO INTERVAL_TBL (f1) VALUES (interval '2' second);
INSERT INTO INTERVAL_TBL (f1) VALUES (interval '3' second);
INSERT INTO INTERVAL_TBL (f1) VALUES (interval '4' second);
SELECT f1, cast(f1 as int), cast(f1 as dec(8,3)), cast(f1 as real) FROM INTERVAL_TBL; -- error
SELECT count(f1), sum(f1), sum(f1), sum(f1), sum(f1) FROM INTERVAL_TBL;
DROP TABLE INTERVAL_TBL;

CREATE TABLE INTERVAL_TBL (f1 interval month);
INSERT INTO INTERVAL_TBL (f1) VALUES (interval '1' month);
INSERT INTO INTERVAL_TBL (f1) VALUES (interval '2' month);
INSERT INTO INTERVAL_TBL (f1) VALUES (interval '3' month);
INSERT INTO INTERVAL_TBL (f1) VALUES (interval '4' month);
SELECT f1, cast(f1 as int) FROM INTERVAL_TBL; -- error
SELECT f1, cast(f1 as dec(3,0)) FROM INTERVAL_TBL;  -- returns error: types month_interval(3,0) and decimal(3,0) are not equal for column 'f1'
SELECT f1, cast(f1 as real) FROM INTERVAL_TBL;  --  returns error: types month_interval(3,0) and real(24,0) are not equal for column 'f1'
SELECT count(f1), sum(f1), sum(f1), sum(f1), sum(f1) FROM INTERVAL_TBL;
DROP TABLE INTERVAL_TBL;

