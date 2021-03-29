-- test operators
select 2 + 3 as add23, 2 - 3 as sub23, 2 * 3 as mul23, 5 / 2 as div52, 5.0 / 2 as div502, 5 % 4 as mod54;
/*
add23	sub23	mul23	div52	div502	mod54
5	-1	6	2	2.500	1
*/

-- ^ 	exponentiation ?? (associates left to right) 	2.0 ^ 3.0 	8, apparently not
select 2 ^ 3 ;
-- returns 1, so is not exponentiation but bit_xor
plan select 2 ^ 3 ;
-- [ sys.bit_xor(tinyint "2", tinyint "3") ]

select 2.0 ^ 3.0 ;
-- returns 1.0, so is not exponentiation but bit_xor
plan select 2.0 ^ 3.0 ;
-- [ sys.bit_xor(decimal(2,1) "20", decimal(2,1) "30") ]

-- |/ 	square root 	|/ 25.0 	5
select |/ 25.0 ;
-- Error: syntax error, unexpected '|' in: "select |"

-- ||/ 	cube root 	||/ 27.0 	3
select ||/ 27.0 ;
-- Error: syntax error, unexpected CONCATSTRING in: "select ||"

-- ! 	factorial 	5 ! 	120
select 5! ;
-- Error: Unexpected symbol (!)

-- !! 	factorial (prefix operator) 	!! 5 	120
select !! 5 ;
-- Error: Unexpected symbol (!)

-- @ 	absolute value 	@ -5.0 	5
select @ -5.0 ;
-- in Oct2020: Error: syntax error, unexpected AT in: "select @"
-- Error: syntax error, unexpected '-' in: "select @ -"

-- & 	bitwise AND 	91 & 15 	11
select 91 & 15 ;
-- 11

-- | 	bitwise OR 	32 | 3 	35
select 32 | 3 ;
-- 35

-- # 	bitwise XOR 	17 # 5 	20
select 17 # 5 ;   -- note # 5 ; is treated as comment hence only select 17 is executed.
;
-- 17, is not expected. it doesn't return an error as "# 5 ;" part is treated as comment

select 17 ^ 5 ;
-- 20
plan select 17 ^ 5 ;
-- is [ sys.bit_xor(tinyint "17", tinyint "5") ]

select 2 ^ 3 ;
-- 1
plan select 2 ^ 3 ;
-- is [ sys.bit_xor(tinyint "2", tinyint "3") ]

-- ~ 	bitwise NOT 	~1 	-2
select ~1 ;
-- -2
plan select ~1 ;
-- is [ sys.bit_not(tinyint "1") ]

-- << 	bitwise shift left 	1 << 4 	16
select 1 << 4 ;
-- 16
plan select 1 << 4 ;
-- is [ sys.left_shift(tinyint "1", int "4") ]
select left_shift(1, 4) ;	-- 16

-- >> 	bitwise shift right 	8 >> 2 	2
select 8 >> 2 ;
-- 2
plan select 8 >> 2 ;
-- is [ sys.right_shift(tinyint "8", int "2") ]
select right_shift(16, 2) ;	-- 4

SELECT 10 DIV 5;
-- Error: syntax error, unexpected sqlINT, expecting SCOLON in: "select 10 div 5"

select abs(-17.4);
-- 17.4

select bit_and(91, 15), bit_not(1), bit_or(32, 3), bit_xor(17, 5);

select cbrt(2.0);
-- in Oct2020:  1.259921049894873
select sys.cbrt(2.0);
-- in Oct2020: 1.259921049894873
select cbrt(27);
-- in Oct2020: 3.0

select ceil(-42.8), ceiling(-95.3), ceil(-42), ceiling(-95);
select ceil(42.8), ceiling(95.3), ceil(42), ceiling(95);
select floor(-42.8), floor(-95.3), floor(-42), floor(-95);
select floor(42.8), floor(95.3), floor(42), floor(95);

-- function div(a,b) does not exist (sql_div() does exist)
select div(9,4);
-- Error: SELECT: no such binary operator 'div(tinyint,tinyint)'
select div(9,4.0);
-- Error: SELECT: no such binary operator 'div(tinyint,decimal)'
select div(9.0,4.0);
-- Error: SELECT: no such binary operator 'div(decimal,decimal)'
select sys.div(9,4);
-- Error: SELECT: no such binary operator 'div(tinyint,tinyint)'
select sys.div(9.0,4.0);
-- Error: SELECT: no such binary operator 'div(decimal,decimal)'


select degrees(0.5);
-- 28.64788975654116 on Oct2020
-- SELECT: no such unary operator 'degrees(decimal)'
select sys.degrees(0.5);
select degrees(2);
-- 114.59155902616465 on Oct2020
-- SELECT: no such unary operator 'degrees(tinyint)'
select sys.degrees(2);
select degrees(pi());
-- 180.0

select radians(45.0);
-- 0.7853981633974483 on Oct2020
-- SELECT: no such unary operator 'radians(decimal)'
select sys.radians(45.0);
select radians(180);
-- 3.141592653589793

select sys.degrees(sys.radians(45));
select sys.radians(sys.degrees(2));

select exp(1), exp(10), exp(1.00), exp(10.00);
-- 2.718281828459045	22026.465794806718	2.718281828459045	22026.465794806718
select right_shift(8, 2);	-- 2
select sql_min(1.2, 5), least(1.2, 5), sql_max(1.2, 5), greatest(1.2, 5);
-- 1.2	1.2	5.0	5.0
select log(2.0), ln(2), log(64.0, 2), log(2, 64.0), log10(100.0), log2(64.0);
-- On Oct2020: 0.6931471805599453	0.6931471805599453	0.16666666666666669	6.0	2.0	6.0
-- DONE: adjust documentation for log(x, b)
-- On Jun2020: 0.6931471805599453	0.6931471805599453	6.0	0.16666666666666669	2.0	6.0
select mod(5.0, 2.1);	-- 0.8

select pi();	-- 3.141592653589793
select power(2, 5), power(2, 31);	-- 32.0	2.147483648E9
-- select rand();	-- 2144156907
select 2144156907 / 2147483648;	-- 0
-- select rand() / 2147483648;	-- 0
-- select cast(rand() as float) / 2147483648;	-- 0.17324864212423563
plan select CAST(RAND() as float) / 2147483648;
select rand(0);	-- 1868859049
select rand(1);	-- 847553797
select rand(2);	-- 543032001
select rand(-5);	-- 1345532277
select random();
-- Error: SELECT: no such operator 'random'

select right_shift(16, 2), right_shift(8.0, 2), right_shift(8.2, 2);
-- 4 	0.2	0.2
select round(41.538);
-- SELECT: no such unary operator 'round(decimal)'
select round(41.538, 2);		-- 41.540
select round(42.4382e0, 2);	-- 42.44

select scale_down(100, 3), scale_down(100.0, 3), scale_down(100, 6), scale_down(100.0, 6);
-- 33	3.3	17	1.7
-- TODO: log issue: scale_down(100.0, 3) produces incorrect result 3.3 instead of 33. idem for scale_down(100.0, 6)

select scale_up(13, 3);
-- 39
select scale_up(13.0, 3);
-- 390 in Oct2020
-- TypeException:user.s1796_0[5]:'calc.*' undefined in:     X_6:bte := calc.*(X_4:sht, X_5:bte);
select scale_up(13, 3.0);
-- in Oct2020: Error: overflow in calculation 30*13.
-- Error: overflow in calculation 13*30.

select sign(-8.4), sign(-0), sign(+0), sign(+8.4);
-- -1	0	0	1
select sql_add(2, 3.4), sql_sub(5, 7);
-- 5.4	-2
select sql_div(3.4, 2), sql_mul(1.2, 5);
-- 1.700	6.00
select sql_max(1.2, 5), sql_min(1.2, 5);
-- 5.0	1.2
select sql_max(1.2, sql_max(3.3, 5));
-- 5.0
select sql_min(1.2, sql_min(3.3, 5));
-- 1.2
select sql_neg(-2.5), sql_neg(2.5);
-- 2.5	-2.5

select sqrt(2.0);
-- 1.4142135623730951
select sqrt(64);
-- 8.0


select sys.alpha(5.0, 1.2);
-- 1.2045844792437546
select sys.fuse(2, 6);		-- 518
select sys.fuse(256 , 1);	-- 16777217
select sys.ms_round(1.2359, 2, 0);	-- 1.24
select sys.ms_trunc(1.2359, 0);	-- 1.0
select sys.ms_trunc(1.2359, 2);	-- 1.23
select sys.ms_trunc(1.2359, 3);	-- 1.235

select acos(0.54), acos(0.25);	-- 1.000359217,	1.318116072
select asin(1.0), atan(1), atan(0.5), atan(1.0, 2.0);	-- 1.570796327,	0.7853981634,	0.463647609,	0.463647609

select atan2(1.0, 2.0);
-- SELECT: no such binary operator 'atan2(decimal,decimal)'

select cos(12.2), cosh(3.0), cot(16.0);	-- 0.9336336441,	10.067662,	3.326323196
select degrees(0.5), radians(45);	-- 28.64788976	0.7853981634
select pi();	-- 3.141592653589793

select sin(1.4), sin(2), sinh(0), sinh(1.4);
-- 0.9854497299884601	0.9092974268256817	0.0	1.9043015014515339
select tan(1.4), tan(2), tanh(0), tanh(1.4);
-- 5.797883715482887	-2.185039863261519	0.0	0.8853516482022625

create sequence tst20210325;
select get_value_for('sys', 'tst20210325');
-- 1
select next_value_for('sys', 'tst20210325');
-- 1
select get_value_for('sys', 'tst20210325');
-- 2
drop sequence tst20210325;

-- rotate_xor_hash(arg_1 bigint, arg_2 int, arg_3 any)
select rotate_xor_hash(1, 1, '1') ;
-- in Oct2020: Error: TypeException:user.main[7]:'calc.rotate_xor_hash' undefined in:     X_1320374:str := calc.rotate_xor_hash(X_1320368:lng, X_1320370:int, X_1320372:str);
-- Error: TypeException:user.s2272_0[8]:'calc.rotate_xor_hash' undefined in:     X_9:bte := calc.rotate_xor_hash(X_5:lng, X_7:int, X_8:bte);
-- Error: TypeException:user.s1802_0[8]:'calc.rotate_xor_hash' undefined in:     X_9:str := calc.rotate_xor_hash(X_5:lng, X_7:int, X_8:str);
select rotate_xor_hash(cast(1 as bigint), cast(1 as int), '1') ;
-- 2154528971 on Oct2020
-- 2154528971 on Jun2020
-- 15039430859

