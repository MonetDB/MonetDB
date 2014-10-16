--
-- float
--

CREATE TABLE FLOAT4_TBL (f1  real);

INSERT INTO FLOAT4_TBL(f1) VALUES ('    0.0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1004.30   ');
INSERT INTO FLOAT4_TBL(f1) VALUES ('     -34.84    ');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e+20');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e-20');

-- test for over and under flow 
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e40');
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e40');
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e-40');
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e-40');

-- bad input
INSERT INTO FLOAT4_TBL(f1) VALUES ('       ');
INSERT INTO FLOAT4_TBL(f1) VALUES ('xyz');
INSERT INTO FLOAT4_TBL(f1) VALUES ('5.0.0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('5 . 0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('5.   0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('     - 3.0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('123            5');

-- special inputs
SELECT cast('NaN' as real);
SELECT cast('nan' as real);
SELECT cast('   NAN  ' as real);
SELECT cast('infinity' as real);
SELECT cast('          -INFINiTY   ' as real);
-- bad special inputs
SELECT cast('N A N' as real);
SELECT cast('NaN x' as real);
SELECT cast(' INFINITY    x' as real);

SELECT cast('Infinity' as real) + 100.0;
SELECT cast('Infinity' as real) / cast('Infinity' as real);
SELECT cast('nan' as real) / cast('nan' as real);


SELECT '' AS five, FLOAT4_TBL.* FROM FLOAT4_TBL;

SELECT '' AS four, f.* FROM FLOAT4_TBL f WHERE f.f1 <> '1004.3';

SELECT '' AS one, f.* FROM FLOAT4_TBL f WHERE f.f1 = '1004.3';

SELECT '' AS three, f.* FROM FLOAT4_TBL f WHERE '1004.3' > f.f1;

SELECT '' AS three, f.* FROM FLOAT4_TBL f WHERE  f.f1 < '1004.3';

SELECT '' AS four, f.* FROM FLOAT4_TBL f WHERE '1004.3' >= f.f1;

SELECT '' AS four, f.* FROM FLOAT4_TBL f WHERE  f.f1 <= '1004.3';

SELECT '' AS three, f.f1, f.f1 * '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 + '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 / '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

SELECT '' AS three, f.f1, f.f1 - '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0';

-- test divide by zero
SELECT '' AS bad, f.f1 / '0.0' from FLOAT4_TBL f;

SELECT '' AS five, * FROM FLOAT4_TBL;

-- test the unary float4abs operator 
SELECT '' AS five, f.f1, @f.f1 AS abs_f1 FROM FLOAT4_TBL f;

UPDATE FLOAT4_TBL
   SET f1 = FLOAT4_TBL.f1 * '-1'
   WHERE FLOAT4_TBL.f1 > '0.0';

SELECT '' AS five, * FROM FLOAT4_TBL;

-- cleanup created table
DROP TABLE FLOAT4_TBL;
