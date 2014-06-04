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

SELECT '' AS four, i.* FROM INT4_TBL i WHERE i.f1 <> smallint '0';

SELECT '' AS four, i.* FROM INT4_TBL i WHERE i.f1 <> integer '0';

SELECT '' AS one, i.* FROM INT4_TBL i WHERE i.f1 = smallint '0';

SELECT '' AS one, i.* FROM INT4_TBL i WHERE i.f1 = integer '0';

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 < smallint '0';

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 < integer '0';

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 <= smallint '0';

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 <= integer '0';

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 > smallint '0';

SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 > integer '0';

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= smallint '0';

SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= integer '0';

-- positive odds
SELECT '' AS one, i.* FROM INT4_TBL i WHERE (i.f1 % smallint '2') = smallint '1';

-- any evens
SELECT '' AS three, i.* FROM INT4_TBL i WHERE (i.f1 % integer '2') = smallint '0';

SELECT '' AS five, i.f1, i.f1 * smallint '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 * smallint '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

SELECT '' AS five, i.f1, i.f1 * integer '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 * integer '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

SELECT '' AS five, i.f1, i.f1 + smallint '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 + smallint '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

SELECT '' AS five, i.f1, i.f1 + integer '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 + integer '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

SELECT '' AS five, i.f1, i.f1 - smallint '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 - smallint '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT '' AS five, i.f1, i.f1 - integer '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 - integer '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT '' AS five, i.f1, i.f1 / smallint '2' AS x FROM INT4_TBL i;

SELECT '' AS five, i.f1, i.f1 / integer '2' AS x FROM INT4_TBL i;

--
-- more complex expressions
--

-- variations on unary minus parsing
SELECT -2+3 AS one;

SELECT 4-2 AS two;

SELECT 2- -1 AS three;

SELECT 2 - -2 AS four;

SELECT smallint '2' * smallint '2' = smallint '16' / smallint '4' AS true;

SELECT integer '2' * smallint '2' = smallint '16' / integer '4' AS true;

SELECT smallint '2' * integer '2' = integer '16' / smallint '4' AS true;

SELECT integer '1000' < integer '999' AS false;

SELECT 4! AS twenty_four;

SELECT !!3 AS six;

SELECT 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS ten;

SELECT 2 + 2 / 2 AS three;

SELECT (2 + 2) / 2 AS two;
