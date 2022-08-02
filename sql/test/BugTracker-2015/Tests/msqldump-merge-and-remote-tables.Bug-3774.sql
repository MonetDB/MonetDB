CREATE TABLE t1 (i int);
CREATE MERGE TABLE mt1 (t int);
ALTER TABLE mt1 ADD TABLE t1;
CREATE REMOTE  TABLE rt1 (t int)  on 'mapi:monetdb://localhost:50000/test';
\d
\d mt1
\d rt1
\D

DROP TABLE rt1;
DROP TABLE mt1;
DROP TABLE t1;
