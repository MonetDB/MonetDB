CREATE TABLE test02 (
id    INTEGER      NOT NULL,
col1  VARCHAR(10)  NOT NULL,
col2  INTEGER      NOT NULL
) ;

INSERT INTO test02 VALUES (10,'ABC',21) ;
INSERT INTO test02 VALUES (11,'ABD',22) ;
INSERT INTO test02 VALUES (12,'ABE',23) ;
INSERT INTO test02 VALUES (13,'ABF',24) ;
INSERT INTO test02 VALUES (14,'ABC',25) ;
INSERT INTO test02 VALUES (15,'ABD',26) ;
INSERT INTO test02 VALUES (16,'ABE',27) ;
INSERT INTO test02 VALUES (17,'ABD',28) ;
INSERT INTO test02 VALUES (18,'ABE',29) ;
INSERT INTO test02 VALUES (19,'ABF',30) ;

plan select col1
from test02
group by col1
order by count(col1);

select col1
from test02
group by col1
order by count(col1);

drop table test02;
