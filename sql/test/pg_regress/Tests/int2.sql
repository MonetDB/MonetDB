--
-- smallint
-- NOTE: smallint operators never check for over/underflow!
-- Some of these answers are consequently numerically incorrect.
--

CREATE TABLE INT2_TBL(f1 smallint);

INSERT INTO INT2_TBL(f1) VALUES ('0   ');

INSERT INTO INT2_TBL(f1) VALUES ('  1234 ');

INSERT INTO INT2_TBL(f1) VALUES ('    -1234');

INSERT INTO INT2_TBL(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT2_TBL(f1) VALUES ('32767');

INSERT INTO INT2_TBL(f1) VALUES ('-32767');

-- bad input values -- should give errors
INSERT INTO INT2_TBL(f1) VALUES ('100000');
INSERT INTO INT2_TBL(f1) VALUES ('asdf');
INSERT INTO INT2_TBL(f1) VALUES ('    ');
INSERT INTO INT2_TBL(f1) VALUES ('- 1234');
INSERT INTO INT2_TBL(f1) VALUES ('4 444');
INSERT INTO INT2_TBL(f1) VALUES ('123 dt');
INSERT INTO INT2_TBL(f1) VALUES ('');

-- postgress syntax, will give error in MonetDB
SELECT '' AS five, INT2_TBL.*;

SELECT '' AS five, * FROM INT2_TBL;

SELECT '' AS four, i.* FROM INT2_TBL i WHERE i.f1 <> cast('0' as smallint);

SELECT '' AS four, i.* FROM INT2_TBL i WHERE i.f1 <> cast('0' as integer);

SELECT '' AS one, i.* FROM INT2_TBL i WHERE i.f1 = cast('0' as smallint);

SELECT '' AS one, i.* FROM INT2_TBL i WHERE i.f1 = cast('0' as integer);

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 < cast('0' as smallint);

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 < cast('0' as integer);

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 <= cast('0' as smallint);

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 <= cast('0' as integer);

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 > cast('0' as smallint);

SELECT '' AS two, i.* FROM INT2_TBL i WHERE i.f1 > cast('0' as integer);

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 >= cast('0' as smallint);

SELECT '' AS three, i.* FROM INT2_TBL i WHERE i.f1 >= cast('0' as integer);

-- positive odds 
SELECT '' AS one, i.* FROM INT2_TBL i WHERE (i.f1 % cast('2' as smallint)) = cast('1' as smallint);

-- any evens 
SELECT '' AS three, i.* FROM INT2_TBL i WHERE (i.f1 % cast('2' as integer)) = cast('0' as smallint);

SELECT '' AS five, i.f1, i.f1 * cast('2' as smallint) AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 * cast('2' as smallint) AS x FROM INT2_TBL i
WHERE abs(f1) < 16384;

SELECT '' AS five, i.f1, i.f1 * cast('2' as integer) AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 + cast('2' as smallint) AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 + cast('2' as smallint) AS x FROM INT2_TBL i
WHERE f1 < 32766;

SELECT '' AS five, i.f1, i.f1 + cast('2' as integer) AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 - cast('2' as smallint) AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 - cast('2' as smallint) AS x FROM INT2_TBL i
WHERE f1 > -32767;

SELECT '' AS five, i.f1, i.f1 - cast('2' as integer) AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 / cast('2' as smallint) AS x FROM INT2_TBL i;

SELECT '' AS five, i.f1, i.f1 / cast('2' as integer) AS x FROM INT2_TBL i;

-- cleanup created table INT2_DBL is done in numerology.sql
