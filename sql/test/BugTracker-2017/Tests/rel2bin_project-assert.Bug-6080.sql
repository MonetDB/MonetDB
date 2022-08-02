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
  64 as c0
from 
  (select distinct 
        68 as c0
      from 
        tab0 as ref_0
          inner join tab1 as ref_1
              right join tab2 as ref_2
              on (ref_1.col0 = ref_2.col0 )
            left join integers as ref_12
              right join another_T as ref_13
              on (ref_12.i = ref_13.col1 )
            on (ref_1.col0 = ref_13.col2 )
          on (ref_0.col0 = ref_13.col2 )
      where (ref_2.col1 is not NULL) 
        and (false)) as subq_0
where subq_0.c0 is not NULL; --empty

ROLLBACK;
