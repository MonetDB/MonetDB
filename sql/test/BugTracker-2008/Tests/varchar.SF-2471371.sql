CREATE TABLE myvar_test (
  col1  VARCHAR(1),
  col2  VARCHAR(2)
) ;

COPY 1 RECORDS INTO myvar_test FROM stdin USING DELIMITERS '|','\n' NULL as '';
a|1b

COPY 1 RECORDS INTO myvar_test FROM stdin USING DELIMITERS '|','\n' NULL as '';
a|b2b

COPY 1 RECORDS INTO myvar_test FROM stdin USING DELIMITERS '|','\n' NULL as '';
aa|bb

COPY 1 RECORDS INTO myvar_test FROM stdin USING DELIMITERS '|','\n' NULL as '';
a|b

select * from myvar_test;

DROP   TABLE myvar_test ;
