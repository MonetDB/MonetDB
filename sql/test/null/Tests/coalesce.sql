CREATE TABLE my_table1 ( col1 VARCHAR(1) ) ;
CREATE TABLE my_table2 ( col1 VARCHAR(1) NOT NULL, col2 INT NOT NULL) ;

INSERT INTO my_table1 VALUES ('A') ;
INSERT INTO my_table1 VALUES ('B') ;
INSERT INTO my_table1 VALUES ('B') ;
INSERT INTO my_table1 VALUES ('C') ;
INSERT INTO my_table1 VALUES ('C') ;
INSERT INTO my_table1 VALUES ('C') ;
INSERT INTO my_table1 VALUES (NULL) ;
INSERT INTO my_table1 VALUES (NULL) ;
INSERT INTO my_table1 VALUES (NULL) ;
INSERT INTO my_table1 VALUES (NULL) ;

INSERT INTO my_table2 VALUES ('A', 1) ;
INSERT INTO my_table2 VALUES ('B', 2) ;
INSERT INTO my_table2 VALUES ('C', 3) ;

SELECT CASE WHEN COALESCE(x.col1,'$CS$') = '$CS$' THEN 0
            WHEN COAlesce(X.Col1,'$CS$') = x.col1 THEN (SELECT y.col2
FROM my_table2 y WHERE y.col1 = x.col1)
       END AS col1
FROM (
      SELECT * FROM my_table1
     ) x
;

DROP   TABLE my_table1 ;
DROP   TABLE my_table2 ;
