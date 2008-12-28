start transaction;
CREATE TABLE my_copytest (
col1 INT,
col2 INT,
col3 INT,
col4 VARCHAR(1),
col5 VARCHAR(1)
) ;

COPY 3 RECORDS INTO my_copytest FROM stdin USING DELIMITERS '|','\n'
NULL as '';
123|1.01||aaa|bbb
553|.02||aaa|bbb
223|2.03||aaa|bbb

select * from my_copytest;
abort;
