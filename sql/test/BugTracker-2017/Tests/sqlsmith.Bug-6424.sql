START TRANSACTION;
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);
CREATE TABLE analytics (aa INT, bb INT, cc BIGINT);
INSERT INTO analytics VALUES (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);

select  
  ref_2.col7 as c0, 
  case when true then ref_3.col0 else ref_3.col0 end
     as c1
from 
  another_T as ref_0
      inner join integers as ref_1
          right join another_T as ref_2
          on ((ref_2.col1 is NULL) 
              or (ref_2.col2 is not NULL))
        inner join tab1 as sample_1
        on (false)
      on (ref_0.col8 = ref_2.col3 )
    inner join tab0 as ref_3
    on (true)
where ref_2.col4 is NULL;

ROLLBACK;
