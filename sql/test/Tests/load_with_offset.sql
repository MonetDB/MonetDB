CREATE TABLE my_test (
  col1  INT,
  col2  INT,
  col3  INT,
  col4  VARCHAR(1),
  col5  VARCHAR(1)
) ;

COPY 5 OFFSET 5 RECORDS INTO my_test FROM stdin USING DELIMITERS '|','\n' NULL as '';
123|1||a|b
553|0||a|b
223|2||a|b
123|0||a|b
823|3||a|b
123|0||a|b
590|0||a|b
239|0||a|b
445|28||a|b

select * from my_test;

COPY 9 RECORDS INTO my_test FROM stdin USING DELIMITERS '|','\n' NULL as '';
123|1||a|b
553|0||a|b
223|2||a|b
123|0||a|b
823|3||a|b
123|0||a|b
590|0||a|b
239|0||a|b
445|28||a|b

select * from my_test;

DROP   TABLE my_test ;
