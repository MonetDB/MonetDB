/* Test without data */
CREATE TABLE test1 ( id INT, name VARCHAR(20));
CREATE TABLE test2 ( id INT, name VARCHAR(20));

SELECT (SELECT COUNT(id) AS cnt1 
          FROM test1 
       ) AS c1
      ,(SELECT COUNT(b.id) AS cnt2 
          FROM test2 b 
         WHERE a.id = b.id
       ) AS c2
  FROM test1 a
;

SELECT (SELECT COUNT(id) AS cnt1 
          FROM test1 
       )
      ,(SELECT COUNT(b.id) AS cnt2 
          FROM test2 b 
         WHERE a.id = b.id
       )
  FROM test1 a
;

SELECT (SELECT COUNT(id) AS cnt1 
          FROM test1 
       )
      ,(SELECT COUNT(b.id) AS cnt2 
          FROM test2 b 
         WHERE a.id = b.id
       ) AS c2
  FROM test1 a
;

SELECT (SELECT COUNT(b.id) AS cnt2 
          FROM test2 b 
         WHERE a.id = b.id
       ) AS c2
      ,(SELECT COUNT(id) AS cnt1 
          FROM test1 
       ) AS c1
  FROM test1 a
;

SELECT (SELECT COUNT(id) AS cnt1 
          FROM test1 
       ) AS c1
      ,(SELECT COUNT(b.id) AS cnt2 
          FROM test2 b 
         WHERE a.id = b.id
       ) AS c2
  FROM test1 a
;

DROP TABLE test1;
DROP TABLE test2;

/* Test with data */
CREATE TABLE test1 ( id INT, name VARCHAR(20));
insert INTO test1 values (1,'name1'),(2,'name2'),(3,'name3');

CREATE TABLE test2 ( id INT, name VARCHAR(20));
insert INTO test2 values (3,'name3'),(4,'name4');

SELECT (SELECT COUNT(id) AS cnt1 
          FROM test1 
       ) AS c1
      ,(SELECT COUNT(b.id) AS cnt2 
          FROM test2 b 
         WHERE a.id = b.id
       ) AS c2
  FROM test1 a
;
SELECT (SELECT COUNT(id) AS cnt1 
          FROM test1 
       )
      ,(SELECT COUNT(b.id) AS cnt2 
          FROM test2 b 
         WHERE a.id = b.id
       )
  FROM test1 a
;

SELECT (SELECT COUNT(id) AS cnt1 
          FROM test1 
       )
      ,(SELECT COUNT(b.id) AS cnt2 
          FROM test2 b 
         WHERE a.id = b.id
       ) AS c2
  FROM test1 a
;

SELECT (SELECT COUNT(b.id) AS cnt2 
          FROM test2 b 
         WHERE a.id = b.id
       ) AS c2
      ,(SELECT COUNT(id) AS cnt1 
          FROM test1 
       ) AS c1
  FROM test1 a
;

SELECT (SELECT COUNT(id) AS cnt1 
          FROM test1 
       ) AS c1
      ,(SELECT COUNT(b.id) AS cnt2 
          FROM test2 b 
         WHERE a.id = b.id
       ) AS c2
  FROM test1 a
;

DROP TABLE test1;
DROP TABLE test2;

