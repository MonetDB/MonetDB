--
-- INTERVAL
--

/* SET DATESTYLE = 'ISO'; */

-- check acceptance of "time zone style"
SELECT cast('01:00' as interval second) AS "One hour";
SELECT cast('+02:00' as interval second) AS "Two hours";
SELECT cast('-08:00' as interval second) AS "Eight hours";
SELECT cast('-05' as interval second) AS "Five hours";
SELECT cast('-1 +02:03' as interval second) AS "22 hours ago...";
SELECT cast('-1 days +02:03' as interval second) AS "22 hours ago...";
SELECT cast('10 years -11 month -12 days +13:14' as interval second) AS "9 years...";

CREATE TABLE INTERVAL_TBL (f1 interval second);

INSERT INTO INTERVAL_TBL (f1) VALUES ('1.2345');
INSERT INTO INTERVAL_TBL (f1) VALUES (60 * 60 * 24 * 365 * 2014);
INSERT INTO INTERVAL_TBL (f1) VALUES ('123456789012345678');
INSERT INTO INTERVAL_TBL (f1) VALUES ('1234567890123456789');
INSERT INTO INTERVAL_TBL (f1) VALUES ('12345678901234567890');

INSERT INTO INTERVAL_TBL (f1) VALUES ('1 day 2 hours 3 minutes 4 seconds');
INSERT INTO INTERVAL_TBL (f1) VALUES ('6 years');
INSERT INTO INTERVAL_TBL (f1) VALUES ('5 months');
INSERT INTO INTERVAL_TBL (f1) VALUES ('5 months 12 hours');

INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 1 minute');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 5 hour');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 10 day');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 34 year');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 3 months');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 14 seconds ago');

-- badly formatted interval
INSERT INTO INTERVAL_TBL (f1) VALUES ('badly formatted interval');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 30 eons ago');

SELECT '' AS ten, INTERVAL_TBL.* FROM INTERVAL_TBL;

-- test interval operators
SELECT '' AS nine, INTERVAL_TBL.* FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 <> cast('@ 10 days' as interval second);

SELECT '' AS three, INTERVAL_TBL.* FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 <= cast('@ 5 hours' as interval second);

SELECT '' AS three, INTERVAL_TBL.* FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 < cast('@ 1 day' as interval second);

SELECT '' AS one, INTERVAL_TBL.* FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 = cast('@ 34 years' as interval second);

SELECT '' AS five, INTERVAL_TBL.* FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 >= cast('@ 1 month' as interval second);

SELECT '' AS nine, INTERVAL_TBL.* FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 > cast('@ 3 seconds ago' as interval second);

SELECT '' AS fortyfive, r1.*, r2.*
   FROM INTERVAL_TBL r1, INTERVAL_TBL r2
   WHERE r1.f1 > r2.f1
   ORDER BY r1.f1, r2.f1;

/* SET DATESTYLE = 'postgres'; */

SELECT '' AS ten, INTERVAL_TBL.* FROM INTERVAL_TBL order by f1 desc;

-- test avg(interval), which is somewhat fragile since people have been
-- known to change the allowed input syntax for type interval without
-- updating pg_aggregate.agginitval

select avg(f1) from interval_tbl;
select avg(cast(f1 as double)) from interval_tbl;
select avg(cast(f1 as decimal)) from interval_tbl;
select avg(cast(f1 as decimal(22,3))) from interval_tbl;

select min(f1) from interval_tbl;
select max(f1) from interval_tbl;

--select sum(f1) from interval_tbl;
select sum(cast(f1 as decimal(22,3))) from interval_tbl;

select count(f1) from interval_tbl;
select count(distinct f1) from interval_tbl;

DROP TABLE INTERVAL_TBL;
