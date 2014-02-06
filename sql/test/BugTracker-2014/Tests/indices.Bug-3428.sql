START TRANSACTION;
CREATE TABLE TEST1 ( one VARCHAR(10) , two INT ) ;
CREATE TABLE TEST2 ( one VARCHAR(10) , two INT ) ;

INSERT INTO TEST1 VALUES ( 'a' , 1 ) ;
INSERT INTO TEST2 VALUES ( 'a' , 1 ) ;

SELECT count(*) FROM ( SELECT one , two FROM TEST1 ) AS a INNER JOIN ( SELECT one , two FROM TEST2 ) AS B on A.one = B.one AND A.two = B.two ;

CREATE INDEX onedex ON TEST1 ( one , two ) ;

SELECT count(*) FROM ( SELECT one , two FROM TEST1 ) AS a INNER JOIN ( SELECT one , two FROM TEST2 ) AS B on A.one = B.one AND A.two = B.two ;
ROLLBACK;