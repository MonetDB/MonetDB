CREATE TABLE test01 ( col1 INT, col2 INT ) ;

INSERT INTO test01 VALUES (1,2) ;
INSERT INTO test01 VALUES (3,4) ;
INSERT INTO test01 VALUES (5,6) ;

select * from test01;

ALTER TABLE test01 ADD COLUMN col3 INT ;

select * from test01;

INSERT INTO test01 VALUES (1,2,3) ;
INSERT INTO test01 VALUES (3,4,5) ;
INSERT INTO test01 VALUES (5,6,7) ;

select * from test01;

ALTER TABLE test01 DROP COLUMN col2 ;

INSERT INTO test01 VALUES (7,8) ;

select * from test01;

DROP   TABLE test01 ;
