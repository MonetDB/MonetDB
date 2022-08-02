START TRANSACTION;
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);

select  
  ref_0.col0 as c0
from 
  tab0 as ref_0
    right join tab1 as sample_5
    on ((true) 
        or (ref_0.col1 is NULL))
where ref_0.col2 is NULL
limit 106;

ROLLBACK;
