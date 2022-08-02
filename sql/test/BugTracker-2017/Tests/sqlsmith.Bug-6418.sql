START TRANSACTION;
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);
CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab2 VALUES (64,77,40), (75,67,58),(46,51,23);

select  
  ref_2.col1 as c0, 
  ref_3.i as c1, 
  sample_0.col0 as c2
from 
  tab0 as sample_0
      right join tab1 as sample_1
      on ((true) 
          or ((sample_0.col0 is NULL) 
            or (sample_0.col1 is not NULL)))
    inner join tab2 as ref_2
      left join integers as ref_3
      on (ref_2.col1 is not NULL)
    on (sample_1.col0 is NULL)
where true
limit 116;

ROLLBACK;
