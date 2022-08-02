START TRANSACTION;

CREATE TABLE tempa ( one double , two integer ) ;
INSERT INTO tempa VALUES ( 1.0 , 1 ) , ( 2.0 , 2 ) ;
SELECT quantile( one , 0.25 ) , quantile( one , 0.5 ) FROM tempa WHERE two > 2 ;

ROLLBACK;
