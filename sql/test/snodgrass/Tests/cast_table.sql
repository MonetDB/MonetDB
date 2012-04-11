-- `<  ',  ` <  = ',  `>  ',  `>=   ', and`<>
CREATE TABLE Employee( Id INTEGER, BirthDate DATE );
INSERT INTO Employee VALUES( 77, '1970-01-01');
INSERT INTO Employee VALUES( 88, '1971-12-13');

SELECT * FROM Employee WHERE CAST( BirthDate AS CHAR(10) )    = '1970-01-01';
SELECT * FROM Employee WHERE CAST( BirthDate AS CHAR(10) ) LIKE '1970-01-01';
SELECT * FROM Employee WHERE CAST( ( DATE '1971-01-01' - BirthDate ) DAY AS INT ) = 365 AND CAST( ( DATE '1971-01-01' - BirthDate ) YEAR AS INT ) =  1; -- fixme

DROP TABLE Employee;
