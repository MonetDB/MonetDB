-- `<  ',  ` <  = ',  `>  ',  `>=   ', and`<>
CREATE TABLE Employee( Id INTEGER, BirthDate DATE );
INSERT INTO Employee VALUES( 77, '1970-01-01');
INSERT INTO Employee VALUES( 88, '1971-12-13');

SELECT * FROM Employee WHERE     BirthDate  =                 DATE '1970-01-01';
-- SELECT * FROM Employee WHERE     BirthDate  =ANY  ( VALUES( ( DATE '1970-01-01') ) ); -- fixme
-- SELECT * FROM Employee WHERE     BirthDate  =ALL  ( VALUES( ( DATE '1970-01-01') ) ); -- fixme
-- SELECT * FROM Employee WHERE     BirthDate  =SOME ( VALUES( ( DATE '1970-01-01') ) ); -- fixme
-- SELECT * FROM Employee WHERE     BirthDate     IN ( VALUES( ( DATE '1970-01-01') ) ); -- fixme
-- SELECT * FROM Employee WHERE NOT BirthDate NOT IN ( VALUES( ( DATE '1970-01-01') ) ); -- fixme
-- SELECT * FROM Employee WHERE     BirthDate  MATCH ( VALUES( ( DATE '1970-01-01') ) ); -- fixme

SELECT * FROM Employee WHERE NOT BirthDate <>                 DATE '1970-01-01';
-- SELECT * FROM Employee WHERE NOT BirthDate <>ANY  ( VALUES( ( DATE '1970-01-01') ) ); -- fixme
-- SELECT * FROM Employee WHERE NOT BirthDate <>ALL  ( VALUES( ( DATE '1970-01-01') ) ); -- fixme
-- SELECT * FROM Employee WHERE NOT BirthDate <>SOME ( VALUES( ( DATE '1970-01-01') ) ); -- fixme

SELECT * FROM Employee WHERE     BirthDate     BETWEEN DATE '1970-01-01' AND DATE '1970-01-01';
SELECT * FROM Employee WHERE NOT BirthDate NOT BETWEEN DATE '1970-01-01' AND DATE '1970-01-01';

DROP TABLE Employee;
