START TRANSACTION;
CREATE TABLE another_T (col1 timestamp, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (timestamp '2018-01-02 08:00:10',2,3,4,5,6,7,8), (timestamp '2018-02-04 19:02:01',22,33,44,55,66,77,88), 
(timestamp '2018-04-19 15:49:45',222,333,444,555,666,777,888), (timestamp '2018-05-03 05:12:04',2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);
CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab2 VALUES (64,77,40), (75,67,58),(46,51,23);

select
  cast(coalesce(ref_0.col1,
    ref_0.col1) as timestamp) as c0,
  ref_0.col2 as c1,
  cast(coalesce(ref_0.col4,
    ref_0.col5) as int) as c2,
  ref_0.col3 as c3,
  ref_0.col1 as c4,
  ref_0.col4 as c5,
  case when cast(nullif(ref_0.col6,
        ref_0.col6) as bigint) is not NULL then ref_0.col5 else ref_0.col5 end
     as c6
from
  another_T as ref_0
where cast(coalesce(ref_0.col6,
    case when EXISTS (
        select
            ref_1.col0 as c0
          from
            tab0 as ref_1
              left join tab1 as ref_2
                right join tab2 as ref_3
                on (true)
              on (ref_1.col1 = ref_2.col0 )
          where true) then ref_0.col3 else ref_0.col3 end
      ) as bigint) is not NULL
limit 101;

ROLLBACK;
