CREATE TABLE t (i1 int, i2 int, s1 string, PRIMARY KEY (i1));

COPY 1 RECORDS INTO t FROM STDIN (i1, s1);
1|abc

select * from t;

DROP table t;
