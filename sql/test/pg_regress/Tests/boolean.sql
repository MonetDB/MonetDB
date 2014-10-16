--
-- BOOLEAN
--

--
-- sanity check - if this fails go insane!
--
SELECT 1 AS one;


-- ******************testing built-in type bool********************

-- check bool type-casting as well as and, or, not in qualifications--

SELECT cast('t' AS boolean) AS "true";
SELECT cast('true' AS boolean) AS true;
SELECT cast('true' AS boolean) AS "true";

SELECT cast('f' AS boolean) AS "false";
SELECT cast('false' AS boolean) AS false;
SELECT cast('false' AS boolean) AS "false";

SELECT cast('t' AS boolean) or cast('f' AS boolean) AS "true";
SELECT cast('true' AS boolean) or cast('false' AS boolean) AS "true";

SELECT cast('t' AS boolean) and cast('f' AS boolean) AS "false";
SELECT cast('true' AS boolean) and cast('false' AS boolean) AS "false";

SELECT not cast('f' AS boolean) AS "true";
SELECT not cast('false' AS boolean) AS "true";
SELECT not cast('true' AS boolean) AS "false";

SELECT cast('t' AS boolean) = cast('f' AS boolean) AS "false";
SELECT cast('true' AS boolean) = cast('false' AS boolean) AS "false";

SELECT cast('t' AS boolean) <> cast('f' AS boolean) AS "true";
SELECT cast('true' AS boolean) <> cast('false' AS boolean) AS "true";


CREATE TABLE BOOLTBL1 (f1 bool);

INSERT INTO BOOLTBL1 (f1) VALUES (cast('t' AS boolean));
INSERT INTO BOOLTBL1 (f1) VALUES (cast('true' AS boolean));

INSERT INTO BOOLTBL1 (f1) VALUES (cast('True' AS boolean));
INSERT INTO BOOLTBL1 (f1) VALUES (cast(lower('True') AS boolean));

INSERT INTO BOOLTBL1 (f1) VALUES ('true');


-- BOOLTBL1 should be full of true's at this string 
SELECT '' AS f_3, * FROM BOOLTBL1;

SELECT '' AS t_3, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = cast('true' AS boolean);

SELECT '' AS t_3, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 <> cast('false' AS boolean);

SELECT '' AS zero, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE booleq(cast('false' AS boolean), f1);

INSERT INTO BOOLTBL1 (f1) VALUES (cast('f' AS boolean));
INSERT INTO BOOLTBL1 (f1) VALUES (cast('false' AS boolean));

SELECT '' AS f_1, BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = cast('false' AS boolean);


CREATE TABLE BOOLTBL2 (f1 bool);

INSERT INTO BOOLTBL2 (f1) VALUES (cast('f' AS boolean));

INSERT INTO BOOLTBL2 (f1) VALUES (cast('false' AS boolean));

INSERT INTO BOOLTBL2 (f1) VALUES (cast('False' AS boolean));
INSERT INTO BOOLTBL2 (f1) VALUES (cast(lower('False') AS boolean));

INSERT INTO BOOLTBL2 (f1) VALUES (cast('FALSE' AS boolean));
INSERT INTO BOOLTBL2 (f1) VALUES (cast(lower('FALSE') AS boolean));

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
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 and BOOLTBL1.f1 = cast('false' AS boolean);


SELECT '' AS tf_12_ff_4, BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 or BOOLTBL1.f1 = cast('true' AS boolean)
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

SELECT '' AS "Not False", f1
   FROM BOOLTBL1
   WHERE NOT FALSE = f1;

SELECT '' AS "Not False", f1
   FROM BOOLTBL1
   WHERE f1 = (NOT FALSE);

SELECT '' AS "False", f1
   FROM BOOLTBL1
   WHERE f1 = FALSE;

SELECT '' AS "Not True", f1
   FROM BOOLTBL1
   WHERE f1 = NOT TRUE;

SELECT '' AS "Not True", f1
   FROM BOOLTBL1
   WHERE NOT TRUE = f1;

SELECT '' AS "Not True", f1
   FROM BOOLTBL1
   WHERE f1 = (NOT TRUE);

SELECT '' AS "True", f1
   FROM BOOLTBL2
   WHERE f1 = TRUE;

SELECT '' AS "Not False", f1
   FROM BOOLTBL2
   WHERE f1 = NOT FALSE;

SELECT '' AS "Not False", f1
   FROM BOOLTBL2
   WHERE NOT FALSE = f1;

SELECT '' AS "Not False", f1
   FROM BOOLTBL2
   WHERE f1 = (NOT FALSE);

SELECT '' AS "False", f1
   FROM BOOLTBL2
   WHERE f1 = FALSE;

SELECT '' AS "Not True", f1
   FROM BOOLTBL2
   WHERE f1 = NOT TRUE;

SELECT '' AS "Not True", f1
   FROM BOOLTBL2
   WHERE NOT TRUE = f1;

SELECT '' AS "Not True", f1
   FROM BOOLTBL2
   WHERE f1 = (NOT TRUE);

--
-- Clean up
-- Many tables are retained by the regression test, but these do not seem
--  particularly useful so just get rid of them for now.
--  - thomas 1997-11-30
--

DROP TABLE  BOOLTBL1;

DROP TABLE  BOOLTBL2;
