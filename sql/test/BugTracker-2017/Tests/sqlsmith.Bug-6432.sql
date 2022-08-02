START TRANSACTION;
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);
CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab2 VALUES (64,77,40), (75,67,58),(46,51,23);

select  
  ref_0.i as c0
from 
  integers as ref_0
      inner join tab0 as ref_1
        inner join tab1 as ref_2
          inner join tab2 as sample_0
          on (sample_0.col1 is NULL)
        on ((ref_1.col0 is NULL) 
            or (ref_2.col1 is not NULL))
      on (12 is NULL)
    left join another_t as ref_3
    on (ref_1.col1 = ref_3.col1 )
where ref_3.estimate is not NULL
limit 156;

ROLLBACK;
