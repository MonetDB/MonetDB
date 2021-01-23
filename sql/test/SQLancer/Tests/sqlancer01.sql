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

START TRANSACTION; --Bug 6900
CREATE TABLE "sys"."t0" (
	"c0" DOUBLE
);
COPY 29 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
0.17813417838107104
0.9549819803006688
0.9780564272521242
0.10974679657112596
0.14469007311670268
0.40657910792795704
767899608
1305867053
50
0.4710730955889548
0.052675133884161784
0.196080224553812
0.7984139684271494
-160886149
0.55847644848666
0.2139700012938821
0.6006984400193495
0.14469007311670268
0.8294571584056999
0.857353880253944
0.13593948125817867
0.39845038529139776
1589184525
0.549119773445742
1039732501
0.17914678733737166
0.5721433567835664
1.58919798e+09
0.10009920106256454

select max(agg0) from (select max(all cast(0.18525435 as string)) as agg0 from t0 where cast(scale_down(- (greatest(greatest(631218936, -562663513), cast(-265902058 as int))), cast(0.41370374 as int)) as boolean) 
union all select all max(all cast(0.18525435 as string)) as agg0 from t0 where not (cast(scale_down(- (greatest(greatest(631218936, -562663513), cast(-265902058 as int))), cast(0.41370374 as int)) as boolean))
union all select all max(all cast(0.18525435 as string)) as agg0 from t0 where (cast(scale_down(- (greatest(greatest(631218936, -562663513), cast(-265902058 as int))), cast(0.41370374 as int)) as boolean)) is null) as asdf;

select scale_down(-631218936, cast(0.41370374 as int)); --simplified
ROLLBACK;

START TRANSACTION; --Bug 6901
CREATE TABLE "t0" ("c0" DOUBLE PRECISION NOT NULL,"c1" VARCHAR(64));
INSERT INTO "t0" VALUES (0.10155454782177964, 'FALSE'), (594212101, NULL), (0.5981662672233894, NULL);
SELECT t0.c0 FROM t0 WHERE CAST((0.3101399424332040855034620108199305832386016845703125) NOT IN (0.6446234112932121007588648353703320026397705078125) AS BOOLEAN);
ROLLBACK;

START TRANSACTION; --Bug 6902
CREATE TABLE "t0" ("c0" DOUBLE PRECISION NOT NULL,"c1" VARCHAR(64));
create view v0(c0, c1) as (select all t0.c0, r'epfNW⟚榢tptPbC{5{ZW}6,R' from t0) with check option;
SELECT v0.c0 FROM v0 WHERE (v0.c1) BETWEEN (replace('2', '2', '1')) AND (v0.c1);
	--empty
SELECT v0.c0 FROM v0 WHERE (v0.c1) NOT BETWEEN SYMMETRIC (replace(CAST(CAST(v0.c0 AS INT) AS STRING), v0.c1, replace(CAST(0.1 AS STRING), v0.c1, v0.c1))) AND (v0.c1);
	--empty
ROLLBACK;

START TRANSACTION;
CREATE TABLE t0 (c0 integer DEFAULT CAST(0 AS INT));
ROLLBACK;

START TRANSACTION; -- Bug 6903 and 6904
CREATE TABLE "sys"."t0" ("c0" DOUBLE, "c1" VARCHAR(496));
COPY 20 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
0.1438664733406536	NULL
0.1765302987566959	NULL
NULL	"0.6278315174254069"
0.9937940991911638	"470810.02006337716"
0.925750874406328	" \\Z᳣s}LfFQk呗8Mßßl07VXpgoVDy\015'X\n}呗x逖n\015>⎑s%cQyIQ"
NULL	"0.17744327397271142"
NULL	"y"
NULL	"緦좺VDOKB "
1511423462	"470810.02006337716"
0.5249207035059663	"470810.02006337716"
1	"470810.02006337716"
0.3762066043983153	"470810.02006337716"
-939120682	"c2lT^9BY0smjöA罀A&ß14_[#*r"
0.522051822144801	"0.008675985"
0.5347718465897517	"c2lt^9by0smjöa罀a&ß14_[#*r"
NULL	"true"
NULL	"-844489501"
NULL	"0.44968215744290607"
NULL	".v"
NULL	"-174251119"

SELECT AVG(t0.c0) FROM t0 GROUP BY CAST(t0.c0 AS VARCHAR(32));
SELECT ALL 0.1002352, AVG(ALL t0.c0) FROM t0 GROUP BY CAST(t0.c0 AS STRING(799)), 0.4665444117594173, ((sql_min(+ ("locate"('', 'F', 150648381)), (((-1039870396))*(length(r'u')))))>>(CAST(0.588018201374832 AS INT)));

SELECT ALL VAR_SAMP(ALL abs(t0.c0)) FROM t0 GROUP BY t0.c1 || '2', t0.c1;
SELECT ALL CAST(VAR_SAMP(ALL abs(CAST(t0.c1 AS INT))) AS INT) FROM t0 GROUP BY CAST(t0.c1 AS INT), t0.c1; --error, cannot convert string into int
ROLLBACK;

START TRANSACTION; -- Bug 6905
CREATE TABLE "sys"."t0" ("c0" INTEGER,"c1" DOUBLE);
COPY 15 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
-1498252621	-1498252621
0	NULL
-539579569	-539579569
-108613919	NULL
0	0
8	NULL
1783007739	NULL
498706403	NULL
0	0.6103170716911988
1493854316	0.6198781779645515
-1608947797	1
1	0.8356779050811012
-101868623	0.16442926747225173
1493854316	-1498252621
1	0.9401903184230552

SELECT t0.c0 FROM t0 WHERE (0.9952398693354088) IN (t0.c1, t0.c1);
	--empty (this one is right)
SELECT ALL t0.c0 FROM t0 WHERE NOT ((0.9952398693354088) IN (t0.c1, t0.c1));
	-- 10 rows (this one is right)
SELECT t0.c0 FROM t0 WHERE ((0.9952398693354088) IN (t0.c1, t0.c1)) IS NULL;
	--          0
	-- -108613919
	--          8
	-- 1783007739
	--  498706403
ROLLBACK;

START TRANSACTION; -- Bug 6906
CREATE TABLE t0("c0" DOUBLE PRECISION, "c1" VARCHAR(496));
create view v0(c0, c1) as (select all t0.c0, r'epfNW⟚榢tptPbC{5{ZW}6,R' from t0) with check option;
select 1 from v0 full outer join t0 on (cast(('a') in ('a') as string) ilike v0.c0);
select 1 from v0 full outer join t0 on cast((v0.c1) in (1) as string) like v0.c0;
select cast(sum(all + (cast(t0.c0 as int))) as bigint) from v0 full outer join t0 on ((cast((cast(v0.c1 as boolean)) not in (true, ((t0.c0)=(t0.c0)), cast(1745166981 as boolean)) as string))ilike(v0.c0));
ROLLBACK;

START TRANSACTION; -- Bug 6907
CREATE TABLE t0 (c0 BOOLEAN, c1 CHAR(140));
create view v0(c0) as (select distinct 0.4 from t0 where ((t0.c0)and((((lower(t0.c1))||(((((-69891801)/(1210439951)))+(cast(0.5895729273161221 as int)))))) 
between symmetric (cast(0.5 as string)) and (greatest(t0.c1, ((t0.c1)||(-2045486895)))))));
create view v1(c0, c1) as (select all 0.1, ((((cast(0.2 as int))/(+ (((((-212060493)*(816153822)))|(((2000022046)+(1143103273))))))))%(cast(cast(0.76693934 as int) as int))) from v0 
group by 0.3, cast(- (cast(2.000022046E9 as int)) as boolean), sql_max(cast(cast(v0.c0 as string(336)) as int), length('1')), ((cast(0.19825737 as int))%(-1509376269)), v0.c0
having count(all (0.7) is not null) order by v0.c0 asc nulls first) with check option;
select all cast(sum(all cast(t0.c0 as int)) as bigint) as agg0 from v1 join t0 on cast(cast(v1.c1 as int) as boolean) where  (v1.c1) not in (v1.c1, 0.36970723) is null;
ROLLBACK;

START TRANSACTION; -- Bug 6908
CREATE TABLE t0(c0 BOOLEAN,c1 CHAR(140));
create view v0(c0) as (select distinct 0.4 from t0 where ((t0.c0)and((((lower(t0.c1))||(((((-69891801)/(1210439951)))+(cast(0.5895729273161221 as int)))))) between symmetric (cast(0.3 as string)) and (greatest(t0.c1, ((t0.c1)||(-2045486895)))))));
create view v1(c0) as (select distinct t0.c1 from t0 where t0.c0);
select max(all abs(+ (- (- (-1620427795))))) from v0, t0 join v1 on ((((v1.c0)||(t0.c1)))ilike(v1.c0));
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t1" ("c0" VARCHAR(427),"c1" TIME);
COPY 5 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
"-"	NULL
"0.9494786438610024"	NULL
"MA4DƹXb,⻇멫ho\trYmꈋP-aR"	NULL
NULL	02:45:58
NULL	05:45:05

SELECT MEDIAN(ALL abs(CASE TIMESTAMP '1970-01-24 09:25:06' WHEN TIMESTAMP '1970-01-19 10:35:50' THEN 2 END)) FROM t1 GROUP BY abs(0.8);
SELECT MEDIAN(ALL abs(CASE TIMESTAMP '1970-01-24 09:25:06' WHEN TIMESTAMP '1970-01-19 10:35:50' THEN 2 END)) FROM t1 GROUP BY abs(0.8) HAVING (NOT (MIN(ALL NOT (NOT ((0.7) IS NULL))))) = TRUE
UNION ALL
SELECT MEDIAN(ALL abs(CASE TIMESTAMP '1970-01-24 09:25:06' WHEN TIMESTAMP '1970-01-19 10:35:50' THEN 2 END)) FROM t1 GROUP BY abs(0.8) HAVING NOT ((NOT (MIN(ALL NOT (NOT ((0.7) IS NULL))))) = TRUE)
UNION ALL
SELECT MEDIAN(ALL abs(CASE TIMESTAMP '1970-01-24 09:25:06' WHEN TIMESTAMP '1970-01-19 10:35:50' THEN 2 END)) FROM t1 GROUP BY abs(0.8) HAVING ((NOT (MIN(ALL NOT (NOT ((0.7) IS NULL))))) = TRUE) IS NULL;
ROLLBACK;

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
DROP TABLE integers;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" CHAR(89) NOT NULL,"c1" BOOLEAN,CONSTRAINT "t0_c0_pkey" PRIMARY KEY ("c0"),
	CONSTRAINT "t0_c0_unique" UNIQUE ("c0"),CONSTRAINT "t0_c1_c0_unique" UNIQUE ("c1", "c0"));
COPY 11 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
"熡U"	false
"3"	NULL
"6"	NULL
"0.6714721480805466"	NULL
"true"	true
"OD6N綥"	false
"흷)%^Ae+c蝢"	true
"9"	false
"']iq"	true
"E"	true
"0.5036928534407451"	false

update t0 set c1 = true where t0.c0 = t0.c0 and t0.c1 = t0.c1;
update t0 set c1 = true, c0 = r'.+' where (((("isauuid"(t0.c0))and(((t0.c0)=(t0.c0)))))and(((t0.c1)=(t0.c1))));
ROLLBACK;
