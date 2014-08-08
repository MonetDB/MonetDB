--
-- RELTIME
--

CREATE TABLE RELTIME_TBL (f1 interval second);

INSERT INTO RELTIME_TBL (f1) VALUES ('@ 1 minute');

INSERT INTO RELTIME_TBL (f1) VALUES ('@ 5 hour');

INSERT INTO RELTIME_TBL (f1) VALUES ('@ 10 day');

INSERT INTO RELTIME_TBL (f1) VALUES ('@ 34 year');

INSERT INTO RELTIME_TBL (f1) VALUES ('@ 3 months');

INSERT INTO RELTIME_TBL (f1) VALUES ('@ 14 seconds ago');


-- badly formatted reltimes
INSERT INTO RELTIME_TBL (f1) VALUES ('badly formatted reltime');

INSERT INTO RELTIME_TBL (f1) VALUES ('@ 30 eons ago');

-- test reltime operators

SELECT '' AS six, RELTIME_TBL.* FROM RELTIME_TBL;

SELECT '' AS five, RELTIME_TBL.* FROM RELTIME_TBL
   WHERE RELTIME_TBL.f1 <> cast('@ 10 days' as interval second);

SELECT '' AS three, RELTIME_TBL.* FROM RELTIME_TBL
   WHERE RELTIME_TBL.f1 <= cast('@ 5 hours' as interval second);

SELECT '' AS three, RELTIME_TBL.* FROM RELTIME_TBL
   WHERE RELTIME_TBL.f1 < cast('@ 1 day' as interval second);

SELECT '' AS one, RELTIME_TBL.* FROM RELTIME_TBL
   WHERE RELTIME_TBL.f1 = cast('@ 34 years' as interval second);

SELECT '' AS two, RELTIME_TBL.* FROM RELTIME_TBL
   WHERE RELTIME_TBL.f1 >= cast('@ 1 month' as interval second);

SELECT '' AS five, RELTIME_TBL.* FROM RELTIME_TBL
   WHERE RELTIME_TBL.f1 > cast('@ 3 seconds ago' as interval second);

SELECT '' AS fifteen, r1.*, r2.*
   FROM RELTIME_TBL r1, RELTIME_TBL r2
   WHERE r1.f1 > r2.f1
   ORDER BY r1.f1, r2.f1;

DROP TABLE RELTIME_TBL;
