START TRANSACTION;
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);

select
  subq_0.c3 as c2
from 
  (select  
        sample_0.col1 as c3,
        sample_0.col3 as c7
      from 
        another_T as sample_0
      where true
      limit 134) as subq_0
where (true)
  or ((select col0 from tab0 where col0=97)
       is not NULL);

select
      (select cast("col0" + 1 as boolean) from tab1 where col0=51) as c3,
       subq_0.c0 as c9
from
  (select sample_0.col1 as c0, 7 as c1
      from another_T as sample_0
              where sample_0.col2 is not NULL limit 95) as subq_0
where cast(coalesce(17, subq_0.c1) as int) is not NULL;

ROLLBACK;
