--
-- BOOLEAN
--

--
-- sanity check - if this fails go insane!
--
SELECT 1 AS one;


-- ******************testing built-in type bool********************

-- check bool type-casting as well as and, or, not in qualifications--

--SELECT bool 't' AS true;
SELECT true AS "true";

--SELECT bool 'f' AS false;
SELECT false AS "false";

--SELECT bool 't' or bool 'f' AS true;
SELECT true or false AS "true";

--SELECT bool 't' and bool 'f' AS false;
SELECT true and false AS "false";

--SELECT not bool 'f' AS true;
SELECT not true AS "true";

--SELECT bool 't' = bool 'f' AS false;
SELECT true = false AS "false";

--SELECT bool 't' <> bool 'f' AS true;
SELECT true <> false AS "true";


CREATE TABLE BOOLTBL1 (f1 bool);

--INSERT INTO BOOLTBL1 (f1) VALUES (bool 't');
INSERT INTO BOOLTBL1 (f1) VALUES (true);

--INSERT INTO BOOLTBL1 (f1) VALUES (bool 'True');
INSERT INTO BOOLTBL1 (f1) VALUES (True);

--INSERT INTO BOOLTBL1 (f1) VALUES (bool 'true');
INSERT INTO BOOLTBL1 (f1) VALUES ('true');


-- BOOLTBL1 should be full of true's at this string 
SELECT '' AS f_3, * FROM BOOLTBL1;

SELECT '' AS t_3, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = true;


SELECT '' AS t_3, BOOLTBL1.* 
   FROM BOOLTBL1
   WHERE f1 <> false;

SELECT '' AS zero, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE booleq(false, f1);

INSERT INTO BOOLTBL1 (f1) VALUES (false);

SELECT '' AS f_1, BOOLTBL1.* 
   FROM BOOLTBL1
   WHERE f1 = false;


CREATE TABLE BOOLTBL2 (f1 bool);

--INSERT INTO BOOLTBL2 (f1) VALUES (bool 'f');
INSERT INTO BOOLTBL2 (f1) VALUES (false);

--INSERT INTO BOOLTBL2 (f1) VALUES (bool 'false');
INSERT INTO BOOLTBL2 (f1) VALUES ('false');

--INSERT INTO BOOLTBL2 (f1) VALUES (bool 'False');
INSERT INTO BOOLTBL2 (f1) VALUES (False);

--INSERT INTO BOOLTBL2 (f1) VALUES (bool 'FALSE');
INSERT INTO BOOLTBL2 (f1) VALUES (FALSE);

-- This is now an invalid expression
-- For pre-v6.3 this evaluated to false - thomas 1997-10-23
INSERT INTO BOOLTBL2 (f1) 
   VALUES ('XXX');  

-- BOOLTBL2 should be full of false's at this string 
SELECT '' AS f_4, * FROM BOOLTBL2;


SELECT '' AS tf_12, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 <> BOOLTBL1.f1;


SELECT '' AS tf_12, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE boolne(BOOLTBL2.f1,BOOLTBL1.f1);


SELECT '' AS ff_4, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 and BOOLTBL1.f1 = 'false';


SELECT '' AS tf_12_ff_4, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 or BOOLTBL1.f1 = 'true'
   ORDER BY BOOLTBL1.f1, BOOLTBL2.f1;

--
-- SQL92 syntax
-- Try all combinations to ensure that we get nothing when we expect nothing
-- - thomas 2000-01-04
--

SELECT '' AS "True", f1
   FROM BOOLTBL1
   WHERE f1 = TRUE;

SELECT '' AS "Not False", f1
   FROM BOOLTBL1
   WHERE f1 = NOT FALSE;

SELECT '' AS "False", f1
   FROM BOOLTBL1
   WHERE f1 = FALSE;

SELECT '' AS "Not True", f1
   FROM BOOLTBL1
   WHERE f1 = NOT TRUE;

SELECT '' AS "True", f1
   FROM BOOLTBL2
   WHERE f1 = TRUE;

SELECT '' AS "Not False", f1
   FROM BOOLTBL2
   WHERE f1 = NOT FALSE;

SELECT '' AS "False", f1
   FROM BOOLTBL2
   WHERE f1 = FALSE;

SELECT '' AS "Not True", f1
   FROM BOOLTBL2
   WHERE f1 = NOT TRUE;

--
-- Clean up
-- Many tables are retained by the regression test, but these do not seem
--  particularly useful so just get rid of them for now.
--  - thomas 1997-11-30
--

DROP TABLE  BOOLTBL1;

DROP TABLE  BOOLTBL2;
