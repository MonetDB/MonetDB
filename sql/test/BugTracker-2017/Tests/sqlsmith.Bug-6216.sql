START TRANSACTION;

CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);

select
  ref_11.col1 as c0
from
  (select
            ref_8.col2 as c0,
            ref_8.col2 as c1,
            ref_8.col2 as c2,
            ref_8.col1 as c3,
            ref_8.col1 as c4,
            45 as c5
          from
            another_T as ref_8
          where ref_8.col1 is NULL) as subq_0
      inner join tab0 as ref_9
      on (subq_0.c5 = ref_9.col0 )
    inner join tab1 as ref_11
    on (subq_0.c5 = ref_11.col0 )
where ref_9.col1 is NULL
limit 146; --empty

ROLLBACK;
