--
-- double
--

CREATE TABLE FLOAT8_TBL(f1 double);

INSERT INTO FLOAT8_TBL(f1) VALUES ('    0.0   ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1004.30  ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('   -34.84');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e+200');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e-200');

-- test for underflow and overflow handling
SELECT cast('10e400' as double);
SELECT cast('-10e400' as double);
SELECT cast('10e-400' as double);
SELECT cast('-10e-400' as double);

-- bad input
INSERT INTO FLOAT8_TBL(f1) VALUES ('     ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('xyz');
INSERT INTO FLOAT8_TBL(f1) VALUES ('5.0.0');
INSERT INTO FLOAT8_TBL(f1) VALUES ('5 . 0');
INSERT INTO FLOAT8_TBL(f1) VALUES ('5.   0');
INSERT INTO FLOAT8_TBL(f1) VALUES ('    - 3');
INSERT INTO FLOAT8_TBL(f1) VALUES ('123           5');

-- special inputs
SELECT cast('NaN' as double);
SELECT cast('nan' as double);
SELECT cast('   NAN  ' as double);
SELECT cast('infinity' as double);
SELECT cast('          -INFINiTY   ' as double);
-- bad special inputs
SELECT cast('N A N' as double);
SELECT cast('NaN x' as double);
SELECT cast(' INFINITY    x' as double);

SELECT cast('Infinity' as double) + 100.0;
SELECT cast('Infinity' as double) / cast('Infinity' as double);
SELECT cast('nan' as double) / cast('nan' as double);

SELECT '' AS five, FLOAT8_TBL.* FROM FLOAT8_TBL;

SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE f.f1 <> '1004.3';

SELECT '' AS one, f.* FROM FLOAT8_TBL f WHERE f.f1 = '1004.3';

SELECT '' AS three, f.* FROM FLOAT8_TBL f WHERE '1004.3' > f.f1;

SELECT '' AS three, f.* FROM FLOAT8_TBL f WHERE  f.f1 < '1004.3';

SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE '1004.3' >= f.f1;

SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE  f.f1 <= '1004.3';

SELECT '' AS three, f.f1, f.f1 * '-10' AS x 
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 + '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 / '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 - '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

-- disabled next query as PostgreSQL interprets x ^ y differently (as power(x, y)) than MonetDB (as bit-xor operator)
--SELECT '' AS one, f.f1 ^ '2.0' AS square_f1
--   FROM FLOAT8_TBL f where f.f1 = '1004.3';

-- absolute value 
SELECT '' AS five, f.f1, abs(f1) AS abs_f1 
   FROM FLOAT8_TBL f;

-- truncate 
SELECT '' AS five, f.f1, truncate(f1) AS trunc_f1
   FROM FLOAT8_TBL f;

-- round 
SELECT '' AS five, f.f1, round(f.f1, 0) AS round_f1
   FROM FLOAT8_TBL f;

-- ceil / ceiling
select ceil(f1) as ceil_f1 from float8_tbl f;
select ceiling(f1) as ceiling_f1 from float8_tbl f;

-- floor
select floor(f1) as floor_f1 from float8_tbl f;

-- sign
select sign(f1) as sign_f1 from float8_tbl f;

-- square root 
SELECT sqrt(cast('64' as double)) AS eight;
SELECT |/ cast('64' as double) AS eight;

SELECT '' AS three, f.f1, |/f.f1 AS sqrt_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';
SELECT '' AS three, f.f1, sqrt(f.f1) AS sqrt_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

-- power
SELECT power(cast('144' as double), cast('0.5' as double));

-- take exp of ln(f.f1)   (in MonetDB ln() is implemented as log())
SELECT '' AS three, f.f1, exp(log(f.f1)) AS exp_ln_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

-- cube root 
SELECT ||/ cast('27' as double) AS three;

SELECT '' AS five, f.f1, ||/f.f1 AS cbrt_f1 FROM FLOAT8_TBL f;


SELECT '' AS five, * FROM FLOAT8_TBL;

UPDATE FLOAT8_TBL
   SET f1 = FLOAT8_TBL.f1 * '-1'
   WHERE FLOAT8_TBL.f1 > '0.0';

SELECT '' AS bad, f.f1 * '1e200' from FLOAT8_TBL f;

SELECT '' AS bad, f.f1 ^ '1e200' from FLOAT8_TBL f;

--SELECT '' AS bad, log(f.f1) from FLOAT8_TBL f where f.f1 = '0.0' ;

SELECT '' AS bad, log(-f.f1) from FLOAT8_TBL f where f.f1 < '0.0' ;

SELECT '' AS bad, exp(-f.f1) from FLOAT8_TBL f where f.f1 > '-1000.0';

SELECT '' AS bad, f.f1 / '0.0' from FLOAT8_TBL f;

SELECT '' AS five, * FROM FLOAT8_TBL;

-- test for over- and underflow 
INSERT INTO FLOAT8_TBL(f1) VALUES ('10e400');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-10e400');

INSERT INTO FLOAT8_TBL(f1) VALUES ('10e-400');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-10e-400');

-- maintain external table consistency across platforms
-- delete all values and reinsert well-behaved ones

DELETE FROM FLOAT8_TBL;

INSERT INTO FLOAT8_TBL(f1) VALUES ('0.0');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-34.84');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-1004.30');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-1.2345678901234e+200');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-1.2345678901234e-200');

SELECT '' AS five, * FROM FLOAT8_TBL;

-- cleanup created table
DROP TABLE FLOAT8_TBL;
