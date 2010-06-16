CREATE TABLE my_table1 ( id INT NOT NULL, col1 VARCHAR(1) ) ;
CREATE TABLE my_table2 ( col1 VARCHAR(1) NOT NULL, col2 INT NOT NULL) ;

INSERT INTO my_table1 VALUES (1,  'A') ;
INSERT INTO my_table1 VALUES (2,  'B') ;
INSERT INTO my_table1 VALUES (3,  'B') ;
INSERT INTO my_table1 VALUES (4,  'C') ;
INSERT INTO my_table1 VALUES (5,  'C') ;
INSERT INTO my_table1 VALUES (6,  'C') ;
INSERT INTO my_table1 VALUES (7,  NULL) ;
INSERT INTO my_table1 VALUES (8,  NULL) ;
INSERT INTO my_table1 VALUES (9,  NULL) ;
INSERT INTO my_table1 VALUES (10, NULL) ;

INSERT INTO my_table2 VALUES ('A', 1) ;
INSERT INTO my_table2 VALUES ('B', 2) ;
INSERT INTO my_table2 VALUES ('C', 3) ;

-- Query 1

SELECT id,
       (SELECT y.col2 FROM my_table2 y WHERE y.col1 = x.col1)
FROM (
      SELECT * FROM my_table1
     ) x
ORDER BY id
;

DROP   TABLE my_table1 ;
DROP   TABLE my_table2 ;
