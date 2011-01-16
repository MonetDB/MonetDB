CREATE TABLE my_copytest (
col1 INT,
col2 INT,
col3 INT,
col4 VARCHAR(1),
col5 VARCHAR(1)
) ;

COPY 1 RECORDS INTO my_copytest FROM stdin USING DELIMITERS '|','\n'
NULL as '';
123|1.01||a|b

COPY 1 RECORDS INTO my_copytest FROM stdin USING DELIMITERS '|','\n'
NULL as '';
553|.02||a|b

COPY 1 RECORDS INTO my_copytest FROM stdin USING DELIMITERS '|','\n'
NULL as '';
223|2.03||a|b

COPY 1 RECORDS INTO my_copytest FROM stdin USING DELIMITERS '|','\n'
NULL as '';
223|2||aaa|b

COPY 1 RECORDS INTO my_copytest FROM stdin USING DELIMITERS '|','\n'
NULL as '';
223|3||a|bbb

select * from my_copytest;
drop table my_copytest;
