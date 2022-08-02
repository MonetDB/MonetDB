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
CREATE TABLE tab3(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab3 VALUES (64,77,40), (75,67,58),(46,51,23);

select  
  subq_0.c1 as c0, 
  subq_0.c2 as c1
from 
  (select  
        ref_25.col1 as c0, 
        (select col1 from tbl_ProductSales WHERE col1 = 1)
           as c1, 
        63 as c2, 
        ref_24.col1 as c3, 
        ref_25.col1 as c4, 
        (select col0 from tab3 where col0 = 64)
           as c5
      from 
        tab0 as ref_23
            inner join tab1 as ref_24
              left join tab2 as ref_25
              on (ref_24.col0 = ref_25.col0 )
            on (ref_23.col0 = ref_24.col1 )
          right join integers as ref_26
          on (ref_25.col1 = ref_26.i )
      where EXISTS (
        select distinct 
            ref_27.col1 as c0
          from 
            LongTable as ref_27
              right join another_T as ref_28
              on (ref_27.col2 = ref_28.col1 )
          where ref_27.col2 is not NULL)) as subq_0
where subq_0.c5 is NULL; --empty

ROLLBACK;
