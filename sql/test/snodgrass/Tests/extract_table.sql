-- `<  ',  ` <  = ',  `>  ',  `>=   ', and`<>
CREATE TABLE Employee( Id INTEGER, BirthDate DATE );
INSERT INTO Employee VALUES( 77, '1970-01-01');
INSERT INTO Employee VALUES( 88, '1971-12-13');

SELECT * FROM Employee WHERE EXTRACT( YEAR FROM BirthDate ) = 1970 AND EXTRACT( MONTH FROM BirthDate ) = 1 AND EXTRACT( DAY FROM BirthDate ) = 1;

DROP TABLE Employee;
