CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

START TRANSACTION;
-- Bug 6883
select 1 from integers where (((0.7161494983624688) in (integers.i)) = true) = false;
	-- 1
	-- 1
	-- 1

delete from integers where (((0.7161494983624688) in (integers.i)) = true) = false;
	-- 3 rows affected
select i from integers;
	-- NULL

CREATE TABLE t0 (a1 INT, a2 int, a3 int); --Bug 6884
UPDATE t0 SET a2 = (- (- (t0.a1))), a3 = (ascii(CAST(ascii('}悂Y8K*韖5<c>^n8_X1X|p(''bX') AS STRING(920)))) WHERE NOT (((0.27023923567918073) NOT IN (0.9149447665258329)) = FALSE);
	--0 rows affected

insert into integers(i) values(((length(reverse('8 \rcr੧[bp1eMY쫺4j5s뮯!<Rn4*}')))>>(((- (- (528640982)))&(ascii('528640982')))))); --Bug 6885
SELECT i from integers order by i;
	-- NULL
	-- 0
select reverse(r'8 \rcr੧[bp1eMY쫺4j5s뮯!<Rn4*}');

select true in (1 in (1));
	-- true
INSERT INTO integers VALUES(ascii(CAST(0.1 AS STRING)) IN (-1.186003652E9));

-- Bug 6886
INSERT INTO another_t(col4) VALUES(-589206676), (-1557408577);
DELETE FROM another_t WHERE ((another_t.col8)<=(+ (another_t.col8)));
ALTER TABLE another_t ADD UNIQUE(col8, col1, col6, col3);
ROLLBACK;

START TRANSACTION; --Bug 6888
create view v0(c0) as (select distinct - (tbl_productsales.totalsales) from tbl_productsales, another_t);
select another_t.col1 from another_t, tbl_productsales cross join v0;
ROLLBACK;

-- Bug 6887
SELECT "sys"."replace"(cast(cast(0.8009925043335998 as clob) as clob(169)),cast("sys"."replace"('!','','wtkg춑5,I}楘') as clob),"sys"."concat"("sys"."concat"('?dMHr펔2!FU4Rᔎ%',-1194732688),0.7566860950241294));
SELECT "replace"('!','','wtkg춑5,I}楘'), "replace"('','!','wtkg춑5,I}楘'), "replace"('abc',NULL,'wtkg춑5,I}楘'), "replace"('abc','b',NULL);

-- Bug 6889
select all integers.i from another_t inner join integers on ((another_t.col3)<=(cast((another_t.col4) between symmetric (cast(0.29924480503501805 as int)) and (another_t.col3) as int))) 
where true union all select all integers.i from another_t join integers on ((another_t.col3)<=(cast((another_t.col4) between symmetric (cast(0.29924480503501805 as int)) and (another_t.col3) as int))) 
where not (true) union all select all integers.i from another_t join integers on ((another_t.col3)<=(cast((another_t.col4) between symmetric (cast(0.29924480503501805 as int)) and (another_t.col3) as int))) where (true) is null;
	-- empty
select 1 from another_t join integers on (cast(another_t.col4 between 1 and 2 as int)) where false;
	-- empty

-- Bug 6892
SELECT another_T.col2 FROM tbl_productsales, integers LEFT OUTER JOIN another_T ON another_T.col1 > 1 WHERE another_T.col2 > 1 GROUP BY another_T.col2 HAVING COUNT((another_T.col2) IN (another_T.col2)) > 0;
	-- 22
	-- 222
	-- 2222

SELECT COUNT(1 IN (1));
	-- 1

START TRANSACTION; --Bug 6893
create view v0(c0) as (select distinct - (tbl_productsales.totalsales) from tbl_productsales, another_t);
create view v1(c0) as (select distinct not (not ((((v0.c0)=(1560867215))) = false)) from integers, another_t, tbl_productsales, v0);
create view v2(c0, c1) as (select v1.c0, -3.025584E8 from v0, tbl_productsales, v1, another_t);
create view v3(c0, c1, c2) as (select all 0.9104878744438205107059047804796136915683746337890625, -705263737, 0.7147939 from v2 where (cast(v2.c0 as varchar(32))) is null order by v2.c1 desc, v2.c0 desc);
SELECT v3.c0 FROM v3, v0, tbl_productsales FULL OUTER JOIN v2 ON v2.c0 RIGHT OUTER JOIN integers ON (tbl_productsales.TotalSales) BETWEEN (NULL) AND (v2.c1) JOIN another_t ON v2.c0;
	-- empty
ROLLBACK;

START TRANSACTION; --Bug 6894
insert into tbl_productsales(product_name, totalsales) values (((cast(0.1 as string))||(charindex(cast(((((1)*(-1)))&(((-1)|(-1)))) as string), 
((((rtrim('0.9407860360743894', ''))||(1)))||(cast(1.44041702E9 as string(75)))), 
((- (((1)*(-1))))*(1))))), -1833694753);
select rtrim('0.9407860360743894', '');
ROLLBACK;

-- Bug 6895
SELECT another_t.col1 FROM tbl_productsales CROSS JOIN another_t WHERE ((tbl_productsales.product_category) NOT IN (tbl_productsales.product_category, tbl_productsales.product_category)) IS NULL;
	-- empty

START TRANSACTION; --Bug 6896
CREATE TABLE t0 (c0 int, c1 int);
insert into t0 values (1, 1);
select all t0.c0 from t0 where (1) not in (1.52414541E9, 0.13482326) 
union all select all t0.c0 from t0 where not (1) not in (1.52414541E9, 0.13482326) 
union all select t0.c0 from t0 where (1) not in (1.52414541E9, 0.13482326) is null;

select 1 from t0 where (3 in (1, 2)) is null; --simplified
	-- empty

SELECT 1 FROM t0 WHERE t0.c0 BETWEEN SYMMETRIC (1 IN (2, 1)) AND t0.c0;
	-- empty
ROLLBACK;

START TRANSACTION; --Bug 6897
CREATE TABLE "t0"("c0" DECIMAL(18,3), "c1" BIGINT);
COPY 12 RECORDS INTO "t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
0.244	NULL
0.578	NULL
0.119	NULL
0.773	495026350
0.329	2108706088
0.483	1298757529
0.880	39
0.084	1
0.332	859611948
0.607	NULL
0.835	0
0.455	-1239303309

SELECT MIN(DISTINCT t0.c1) FROM t0 WHERE 1 >= t0.c0 GROUP BY true;
	-- -1239303309
ROLLBACK;

START TRANSACTION; --Bug 6898
CREATE TABLE t0 (c0 int, c1 int);
insert into t0 values (1, 1);
CREATE TABLE v0 (c0 int, c1 int);
insert into v0 values (1, 1);
select all count(all 0.5923759) from v0 left outer join t0 on (('1')||
cast((0.95672141382556563637962199209141544997692108154296875) in (0.93132256561636328484610203304328024387359619140625, t0.c0) as varchar(32)) like('jBlZöx TW9*ࡈxw㟩*'));

select 1 from v0 join t0 on (((substr('1', 1, v0.c0)) || (1) in (1, t0.c0)) like 'a'); --simplified
	-- empty
ROLLBACK;

START TRANSACTION; --Bug 6899
CREATE TABLE "sys"."t0" (
	"c0" DECIMAL(18,3),
	"c1" DOUBLE
);
COPY 25 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
0.861	0.6056174268361689
NULL	0.5542760880735715
0.566	0.5542760880735715
0.923	0.052316013908556114
0.162	0.5542760880735715
0.329	0.44073078327593473
0.662	0.5255985140328766
0.001	NULL
0.736	NULL
0.604	0.5394080603985286
0.988	NULL
0.232	0.23262100818671297
0.041	-7.386201860458207e+20
0.776	NULL
0.805	NULL
0.189	NULL
0.493	NULL
0.164	0.23262100818671297
0.316	0.6870041098637464
NULL	1.19449193e+09
NULL	0.5798864736610635
0.219	37
0.924	0.8488059445245933
0.413	0.6870041098637464
0.234	0

SELECT t0.c0 FROM t0 WHERE NOT (CAST((t0.c1) IS NULL AS BOOLEAN)) UNION ALL SELECT t0.c0 FROM t0 WHERE NOT (NOT (CAST((t0.c1) IS NULL AS BOOLEAN))) UNION ALL SELECT ALL t0.c0 FROM t0 WHERE (NOT (CAST((t0.c1) IS NULL AS BOOLEAN))) IS NULL;
SELECT count(*) FROM t0 WHERE (NOT (CAST((t0.c1) IS NULL AS BOOLEAN))) IS NULL; --simplified
	-- 0
ROLLBACK;

START TRANSACTION;
CREATE TABLE t0 (c0 boolean);
ALTER TABLE t0 ALTER c0 SET DEFAULT (0.1) IS NULL;
ROLLBACK;

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
DROP TABLE integers;
