START TRANSACTION;
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE LongTable (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO LongTable VALUES (1,7,2,1,1,909,1,1), (2,7,2,2,3,4,4,6), (NULL,5,4,81,NULL,5,-10,1), (-90,NULL,0,NULL,2,0,1,NULL);
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);
CREATE TABLE tbl_ProductSales (col1 INT, col2 varchar(64), col3 varchar(64), col4 INT, col5 REAL, col6 date); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200, 1.2, date '2015-12-12'),(2,'Game','PKO Game',400, -1.0, date '2012-02-10'),(3,'Fashion','Shirt',500, NULL, date '1990-01-01'),
(4,'Fashion','Shorts',100, 102.45, date '2000-03-08'),(5,'Sport','Ball',0, 224.78, NULL);
CREATE TABLE analytics (aa INT, bb INT, cc BIGINT);
INSERT INTO analytics VALUES (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);
CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab2 VALUES (64,77,40), (75,67,58),(46,51,23);

select 
 case when subq_1.c1 is not null then subq_1.c0 else subq_1.c0 end
 as c0
from 
 (select 
 ref_0.col2 as c0
 from 
 tab2 as ref_0
 where true
 limit 93) as subq_0
 left join (select 
 ref_1.i as c0, 
 case when (true) 
 or (63 is null) then ref_2.col1 else ref_2.col1 end
 as c1, 
 ref_1.i as c2
 from 
 integers as ref_1
 right join tab0 as ref_2
 on (95 is null)
 where true
 limit 62) as subq_1
 on ((subq_0.c0 is null) 
 and (((subq_0.c0 is not null) 
 or (exists (
 select 
 subq_0.c0 as c0
 from 
 analytics as ref_3
 where (((false) 
 or (subq_1.c1 is not null)) 
 and (true)) 
 and (false)))) 
 and (((false) 
 or ((exists (
 select 
 ref_4.aa as c0, 
 subq_1.c2 as c1, 
 subq_1.c2 as c2, 
 subq_1.c0 as c3, 
 ref_4.cc as c4
 from 
 analytics as ref_4
 where (((true) 
 and (true)) 
 and (ref_4.aa is not null)) 
 and (false))) 
 or (false))) 
 and (subq_1.c0 is null))))
where subq_0.c0 is not null
limit 32; -- 3 NULL values on 3 rows, 1 column

ROLLBACK;
