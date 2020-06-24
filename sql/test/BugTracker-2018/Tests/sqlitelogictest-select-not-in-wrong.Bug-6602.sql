CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab2 VALUES(64,77,40),(75,67,58),(46,51,23);
SELECT * FROM tab2 WHERE + col2 NOT IN ( + - 59 + + ( 76 ), col1, + CAST ( NULL AS INTEGER ), col1, - 19, col1 );
DROP TABLE tab2;

CREATE TABLE CITIES(CITY varchar(50) NULL);
INSERT INTO CITIES
	SELECT 'Paris' UNION ALL
	SELECT 'Montreal' UNION ALL
	SELECT 'New York' UNION ALL
	SELECT NULL;

SELECT 'Found Montreal' WHERE 'Montreal' IN (SELECT city from CITIES);
SELECT 'Found Sydney' WHERE 'Sydney' IN (SELECT city from CITIES);
SELECT 'Sydney Not Found' WHERE 'Sydney' NOT IN (SELECT city from CITIES);
SELECT 'Sydney Not Found' WHERE 'Sydney' NOT IN ('Paris','Montreal','New York');
SELECT 'Sydney Not Found' WHERE 'Sydney' NOT IN ('Paris','Montreal','New York', NULL);
SELECT 'Sydney Not Found' WHERE 'Sydney'<>'Paris' AND 'Sydney'<>'Montreal';
SELECT 'Sydney Not Found' WHERE 'Sydney'<>'Paris' AND 'Sydney'<>'Montreal' AND 'Sydney'<>null;

SELECT city from CITIES WHERE city in (select city from CITIES);
--SELECT 'Sydney Not Found' WHERE NOT EXISTS (SELECT 1/0 FROM CITIES WHERE CITY = 'Sydney');
drop table CITIES;
