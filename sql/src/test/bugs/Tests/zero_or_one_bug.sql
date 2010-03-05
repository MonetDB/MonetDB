CREATE TABLE my_table1 (
--
     id1    INT NOT NULL,
     col1a  VARCHAR(10),
     col1b  VARCHAR(1)
--
) ;

INSERT INTO my_table1 VALUES (1, 'a', '') ;
INSERT INTO my_table1 VALUES (2, 'b', '') ;
INSERT INTO my_table1 VALUES (3, 'c', '') ;
INSERT INTO my_table1 VALUES (4, 'd', '') ;
INSERT INTO my_table1 VALUES (5, 'e', '') ;
INSERT INTO my_table1 VALUES (6, 'f', '') ;
INSERT INTO my_table1 VALUES (7, 'g', '') ;
INSERT INTO my_table1 VALUES (8, 'h', '') ;
INSERT INTO my_table1 VALUES (9, 'i', '') ;

CREATE TABLE my_table2 (
--
     id2    INT NOT NULL,
     col2   VARCHAR(10)

) ;

-- Query 1
--
SELECT
      COALESCE(id1,0),
      (SELECT col2 FROM my_table2 WHERE col2 = col1a)
FROM  my_table1 x
;

-- Query 2
--
SELECT
      COALESCE(id1,0),
      (SELECT col2 FROM my_table2 WHERE col2 = col1a) AS my_col1
FROM  my_table1 x
;

DROP   TABLE my_table1 ;
DROP   TABLE my_table2 ;
