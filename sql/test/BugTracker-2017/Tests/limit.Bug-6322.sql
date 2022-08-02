START TRANSACTION;
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);

select
  subq_0.c3 as c2
from 
  (select  
        sample_0.col0 as c3,
        sample_0.col1 as c7
      from 
        tab0 as sample_0
      where true
      limit 14) as subq_0
where (true)
  or ((select col1 from tab1 where col1=51)
       is not NULL);

ROLLBACK;
