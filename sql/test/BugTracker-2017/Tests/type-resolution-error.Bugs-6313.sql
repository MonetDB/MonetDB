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

select col1 
from tab1
where ((select r_regionkey from sys.region) is NULL)
  or ((select col0 from tab0) is not NULL);

select
          sample_69.col0 as c2
from    
  tab1 as sample_65
	left join tab0 as sample_69
	on (sample_65.col0 = sample_69.col0 )
        where 
        ((select i from integers) is NULL)
          or (cast(coalesce((select col7 from another_T) ,
              sample_65.col1) as boolean) is not NULL);

DROP TABLE another_T;
DROP TABLE integers;
DROP TABLE tab0;
DROP TABLE tab1;
DROP TABLE tab2;
