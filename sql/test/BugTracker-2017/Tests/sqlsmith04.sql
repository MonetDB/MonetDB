-- After 154K queries.

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
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);
CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab2 VALUES (64,77,40), (75,67,58),(46,51,23);

select
  ref_696.col1 as c0,
  ref_699.col2 as c1
from
  tab0 as ref_678
        left join tab1 as ref_695
          right join tab2 as ref_696
            inner join integers as ref_697
              left join another_T as ref_698
              on (ref_697.i = ref_698.col1 )
            on (ref_696.col0 = ref_698.col1 )
          on (ref_695.col0 = ref_698.col1 )
        on (ref_678.col0 = ref_698.col2 )
      left join tbl_ProductSales as ref_699
      on (ref_678.col0 = ref_699.col1 )
    left join LongTable as ref_700
    on (ref_698.col3 = ref_700.col1 )
where ref_700.col2 is not NULL
limit 77; --empty

ROLLBACK;
