START TRANSACTION;
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

select
  ref_2.col1 as c0,
  ref_2.col1 as c1,
  ref_2.col2 as c2,
  90 as c3,
  ref_2.col2 as c4,
  ref_2.col3 as c5
from
  another_T as ref_2
where 24 is NULL; --empty

ROLLBACK;
