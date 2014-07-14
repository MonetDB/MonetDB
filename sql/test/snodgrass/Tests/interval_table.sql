-- `<  ',  ` <  = ',  `>  ',  `>=   ', and`<>
CREATE TABLE Employee( Id INTEGER, BirthDate DATE );
INSERT INTO Employee VALUES( 77, '1970-01-01');
INSERT INTO Employee VALUES( 88, '1971-12-13');

SELECT * FROM Employee WHERE ( BirthDate, INTERVAL '0' DAY ) OVERLAPS ( DATE '1970-01-01', INTERVAL '0' DAY ); -- fixme
SELECT * FROM Employee WHERE ( BirthDate, BirthDate )        OVERLAPS ( DATE '1970-01-01', INTERVAL '0' DAY ); -- fixme
SELECT * FROM Employee WHERE ( BirthDate, INTERVAL '0' DAY ) OVERLAPS ( DATE '1970-01-01', DATE '1970-01-01'); -- fixme
SELECT * FROM Employee WHERE ( BirthDate, BirthDate )        OVERLAPS ( DATE '1970-01-01', DATE '1970-01-01'); -- fixme

SELECT * FROM Employee WHERE ( BirthDate, NULL ) OVERLAPS ( DATE '1970-01-01', INTERVAL '0' DAY ); -- fixme
SELECT * FROM Employee WHERE ( BirthDate, NULL ) OVERLAPS ( DATE '1970-01-01', NULL ); -- fixme
SELECT * FROM Employee WHERE ( BirthDate, NULL ) OVERLAPS ( NULL             , DATE '1970-01-01'); -- fixme

DROP TABLE Employee;
