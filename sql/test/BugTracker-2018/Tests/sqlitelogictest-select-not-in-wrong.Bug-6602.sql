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
SELECT 'Found Sidney' WHERE 'Sidney' IN (SELECT city from CITIES);
SELECT 'Sidney Not Found' WHERE 'Sidney' NOT IN (SELECT city from CITIES);
SELECT 'Sidney Not Found' WHERE 'Sidney' NOT IN ('Paris','Montreal','New York');
SELECT 'Sidney Not Found' WHERE 'Sidney' NOT IN ('Paris','Montreal','New York', NULL);
SELECT 'Sidney Not Found' WHERE 'Sidney'<>'Paris' AND 'Sidney'<>'Montreal';
SELECT 'Sidney Not Found' WHERE 'Sidney'<>'Paris' AND 'Sidney'<>'Montreal' AND 'Sidney'<>null;

SELECT city from CITIES WHERE city in (select city from CITIES);
--SELECT 'Sidney Not Found' WHERE NOT EXISTS (SELECT 1/0 FROM CITIES WHERE CITY = 'Sidney');
drop table CITIES;
