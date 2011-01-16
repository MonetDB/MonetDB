START TRANSACTION;

CREATE SEQUENCE "mytest" AS int;
SELECT q.name, s.name, q."start", q."minvalue", q."maxvalue",	q."increment", q.cacheinc, q."cycle"  FROM SEQUENCES q, SCHEMAS s where s.id = q.schema_id;

SELECT NEXT VALUE FOR "mytest";
SELECT NEXT VALUE FOR "mytest";
SELECT q.name, s.name, q."start", q."minvalue", q."maxvalue",	q."increment", q.cacheinc, q."cycle"  FROM SEQUENCES q, SCHEMAS s where s.id = q.schema_id;

ALTER SEQUENCE "mytest" RESTART WITH 10;
SELECT q.name, s.name, q."start", q."minvalue", q."maxvalue",	q."increment", q.cacheinc, q."cycle"  FROM SEQUENCES q, SCHEMAS s where s.id = q.schema_id;

SELECT NEXT VALUE FOR "mytest";
SELECT NEXT VALUE FOR "mytest";
SELECT q.name, s.name, q."start", q."minvalue", q."maxvalue",	q."increment", q.cacheinc, q."cycle"  FROM SEQUENCES q, SCHEMAS s where s.id = q.schema_id;

DROP SEQUENCE "mytest";

ROLLBACK;
