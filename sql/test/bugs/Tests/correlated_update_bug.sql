
CREATE TABLE t1
  (id INT NOT NULL
  ,c1 DOUBLE NOT NULL
  ,c2 DOUBLE NOT NULL
  ,c3 DOUBLE NOT NULL
  ,c4 DOUBLE NOT NULL
  ,c5 DOUBLE NOT NULL
  ,c6 DOUBLE NOT NULL
  ,c7 DOUBLE NOT NULL
  )
;

CREATE TABLE t2
  (id INT NOT NULL
  ,c1 DOUBLE NOT NULL
  ,c2 DOUBLE NOT NULL
  ,c3 DOUBLE NOT NULL
  ,c4 DOUBLE NOT NULL
  ,c5 DOUBLE NOT NULL
  ,c6 DOUBLE NOT NULL
  ,c7 DOUBLE NOT NULL
  )
;

INSERT INTO t1
VALUES 
  (1,1.0,2.0,3.0,4.0,5.0,6.0,7.0),
  (2,2.0,3.0,4.0,5.0,6.0,7.0,8.0),
  (3,3.0,4.0,5.0,6.0,7.0,8.0,9.0),
  (4,4.0,5.0,6.0,7.0,8.0,9.0,1.0),
  (5,5.0,6.0,7.0,8.0,9.0,1.0,2.0)
;

INSERT INTO t2
VALUES 
  (1,11.0,12.0,13.0,14.0,15.0,16.0,17.0),
  (2,12.0,13.0,14.0,15.0,16.0,17.0,18.0),
  (3,13.0,14.0,15.0,16.0,17.0,18.0,19.0),
  (4,14.0,15.0,16.0,17.0,18.0,19.0,11.0),
  (5,15.0,16.0,17.0,18.0,19.0,11.0,12.0)
;

SELECT * FROM t1;
SELECT * FROM t2;


UPDATE t1
   SET c1 = (SELECT c1
               FROM t2
              WHERE t2.id = t1.id
            )
      ,c2 = (SELECT c2
               FROM t2
              WHERE t2.id = t1.id
            )
      ,c3 = (SELECT c3
               FROM t2
              WHERE t2.id = t1.id
            )
      ,c4 = (SELECT c4
               FROM t2
              WHERE t2.id = t1.id
            )
      ,c5 = (SELECT c5
               FROM t2
              WHERE t2.id = t1.id
            )
      ,c6 = (SELECT c6
               FROM t2
              WHERE t2.id = t1.id
            )
      ,c7 = (SELECT c7
               FROM t2
              WHERE t2.id = t1.id
            )
 WHERE EXISTS (SELECT id    
                 FROM t2
                WHERE t2.id = t1.id
              )
;

SELECT * FROM t1;

DROP TABLE t1;
DROP TABLE t2;
