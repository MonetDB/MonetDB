SELECT * FROM ( VALUES ( 1 , 2 ) , ( 3 , NULL ) ) AS z ( L1 , L2 ) WHERE L2 IN ( 2 , NULL );
SELECT CASE WHEN L2 IN ( 2 , NULL ) THEN 'yes' ELSE 'no' END FROM ( VALUES ( 1 , 2 ) , ( 3 , NULL ) ) AS z ( L1 , L2 ) ;
