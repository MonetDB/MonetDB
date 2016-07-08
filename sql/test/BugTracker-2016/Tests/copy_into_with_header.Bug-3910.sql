create table t (f1 int, f2 int);
COPY 1 RECORDS INTO t(f2,f1) from STDIN USING DELIMITERS ',';
1,2

SELECT * from t;

COPY 1 RECORDS INTO t from STDIN (f2,f1) USING DELIMITERS ',';
1,2

SELECT * from t;

COPY 1 RECORDS INTO t(f1,f2) from STDIN (f2,f1) USING DELIMITERS ',';
1,2

SELECT * from t;

drop table t;
