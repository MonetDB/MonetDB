
CREATE TABLE my_table6 (
--
   my_id         INT,
   col1          VARCHAR(10),
   col2          VARCHAR(10),
   col3          VARCHAR(10)
--
) ;

CREATE TABLE my_table7 (
--
   my_id         INT,
   col1          VARCHAR(10),
   col2          VARCHAR(10),
   col3          VARCHAR(10)
--
) ;

COPY 9 RECORDS INTO my_table6 FROM STDIN DELIMITERS '|','\n','' ;
10|aa||aaa
20|ab|NULL|aab
30|ac||aac
40|ad||aad
50|ae|NULL|aae
60|af||aaf
70|ag|NULL|aag
80|ah||aah
90|ai|NULL|aai


INSERT INTO my_table7 (
--
          my_id,
          col1,
          col2,
          col3
--
)
SELECT
          my_id,
          NULLIF(TRIM(col1),''),
          NULLIF(TRIM(col2),''),
          NULLIF(TRIM(col3),'')
FROM
          my_table6
;

select * from my_table7;

DROP   TABLE my_table6 ;
DROP   TABLE my_table7 ;

