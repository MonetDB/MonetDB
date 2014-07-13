--
-- integer
-- WARNING: integer operators never check for over/underflow!
-- Some of these answers are consequently numerically incorrect.
--

CREATE TABLE INT4_TBL(f1 integer);

INSERT INTO INT4_TBL(f1) VALUES ('   0  ');

INSERT INTO INT4_TBL(f1) VALUES ('123456     ');

INSERT INTO INT4_TBL(f1) VALUES ('    -123456');

INSERT INTO INT4_TBL(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');

INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

-- bad input values -- should give errors
INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');
INSERT INTO INT4_TBL(f1) VALUES ('asdf');
INSERT INTO INT4_TBL(f1) VALUES ('     ');
INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');
INSERT INTO INT4_TBL(f1) VALUES ('- 1234');
INSERT INTO INT4_TBL(f1) VALUES ('123       5');
INSERT INTO INT4_TBL(f1) VALUES ('');


SELECT '' AS five, INT4_TBL.*;

SELECT '' AS four, i.* FROM INT4_TBL i WHERE i.f1 <> cast('0' as smallint);

SELECT '' AS four, i.* FROM INT4_TBL i WHERE i.f1 <> cast('0' as integer);

SELECT '' AS one, i.* FROM INT4_TBL i WHERE i.f1 = cast('0' as smallint);

SELECT '' AS one, i.* FROM INT4_TBL i WHERE i.f1 = cast('0' as integer);

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 < cast('0' as smallint);

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 < cast('0' as integer);

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 <= cast('0' as smallint);

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 <= cast('0' as integer);

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 > cast('0' as smallint);

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 > cast('0' as integer);

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= cast('0' as smallint);

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= cast('0' as integer);

-- positive odds
SELECT '' AS one, i.* FROM INT4_TBL i WHERE (i.f1 % cast('2' as smallint)) = cast('1' as smallint);

-- any evens
SELECT '' AS three, i.* FROM INT4_TBL i WHERE (i.f1 % cast('2' as integer)) = cast('0' as smallint);

SELECT '' AS five, i.f1, i.f1 * cast('2' as smallint) AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 * cast('2' as smallint) AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

SELECT '' AS five, i.f1, i.f1 * cast('2' as integer) AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 * cast('2' as integer) AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

SELECT '' AS five, i.f1, i.f1 + cast('2' as smallint) AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 + cast('2' as smallint) AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

SELECT '' AS five, i.f1, i.f1 + cast('2' as integer) AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 + cast('2' as integer) AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

SELECT '' AS five, i.f1, i.f1 - cast('2' as smallint) AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 - cast('2' as smallint) AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT '' AS five, i.f1, i.f1 - cast('2' as integer) AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 - cast('2' as integer) AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT '' AS five, i.f1, i.f1 / cast('2' as smallint) AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 / cast('2' as integer) AS x FROM INT4_TBL i;

--
-- more complex expressions
--

-- variations on unary minus parsing
SELECT -2+3 AS one;

SELECT 4-2 AS two;

SELECT 2- -1 AS three;

SELECT 2 - -2 AS four;

SELECT cast('2' as smallint) * cast('2' as smallint) = cast('16' as smallint) / cast('4' as smallint) AS "true";

SELECT cast('2' as integer) * cast('2' as smallint) = cast('16' as smallint) / cast('4' as integer) AS "true";

SELECT cast('2' as smallint) * cast('2' as integer) = cast('16' as integer) / cast('4' as smallint) AS "true";

SELECT cast('1000' as integer)  < cast('999' as integer) AS "false";

SELECT 4! AS twenty_four;

SELECT !!3 AS six;

SELECT 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS ten;

SELECT 2 + 2 / 2 AS three;

SELECT (2 + 2) / 2 AS two;

-- cleanup created table INT4_DBL is done in numerology.sql
