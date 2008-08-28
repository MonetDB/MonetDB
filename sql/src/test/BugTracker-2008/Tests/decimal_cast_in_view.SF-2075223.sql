CREATE TABLE A (a CHAR (10));
CREATE VIEW A_VIEW AS
SELECT CAST (a AS NUMERIC (5, 2)) AS b
FROM A;
INSERT INTO A VALUES (' 54.');

-- return -1546.00 (should be 54.00)
select * from A_VIEW;

drop view A_VIEW;
drop table A;
