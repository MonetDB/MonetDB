CREATE TABLE my_test (
  col1  INT,
  col2  INT,
  col3  INT,
  col4  VARCHAR(1),
  col5  VARCHAR(1)
) ;

COPY 5 OFFSET 5 RECORDS INTO my_test FROM stdin USING DELIMITERS '|','\n' ;
123|1.01||aaa|bbb
553|.02||aaa|bbb
223|2.03||aaa|bbb
123|.04||aaa|bbb
823|3.05||aaa|bbb
123|0.06||aaa|bbb
590|0.07||aaa|bbb
239|.08||aaa|bbb
445|28.09||aaa|bbb

select * from my_test;

COPY 9 RECORDS INTO my_test FROM stdin USING DELIMITERS '|','\n' ;
123|1.01||aaa|bbb
553|.02||aaa|bbb
223|2.03||aaa|bbb
123|.04||aaa|bbb
823|3.05||aaa|bbb
123|0.06||aaa|bbb
590|0.07||aaa|bbb
239|.08||aaa|bbb
445|28.09||aaa|bbb

select * from my_test;

DROP   TABLE my_test ;
