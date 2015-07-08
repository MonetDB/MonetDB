CREATE TABLE t1 (i int);
CREATE REMOTE TABLE rt (LIKE t1) ON 'mapi:monetdb://localhost:50000/test';
SELECT * FROM rt;
