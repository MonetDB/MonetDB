CREATE TABLE test01 (
id    INTEGER      NOT NULL,
col1  VARCHAR(10)  NOT NULL,
col2  INTEGER      NOT NULL
) ;

INSERT INTO test01 VALUES (10,'ABC',21) ;
INSERT INTO test01 VALUES (11,'ABD',22) ;
INSERT INTO test01 VALUES (12,'ABE',23) ;
INSERT INTO test01 VALUES (13,'ABF',24) ;
INSERT INTO test01 VALUES (14,'ABC',25) ;
INSERT INTO test01 VALUES (15,'ABD',26) ;
INSERT INTO test01 VALUES (16,'ABE',27) ;
INSERT INTO test01 VALUES (17,'ABD',28) ;
INSERT INTO test01 VALUES (18,'ABE',29) ;
INSERT INTO test01 VALUES (19,'ABF',30) ;

select col1
from test01
group by col1
order by col1;

select col1 as c0
from test01
group by col1
order by c0;

select col1 as c0
from test01
group by col1
order by col1;
