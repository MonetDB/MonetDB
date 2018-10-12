CREATE FUNCTION casttoint(b clob) RETURNS INT BEGIN RETURN cast(b as int); END;

CREATE MERGE TABLE testmeplease (a int, b varchar(32)) PARTITION BY RANGE USING ( casttoint(b) );
CREATE TABLE sub1 (a int, b varchar(32));

UPDATE testmeplease SET a = a + 1 WHERE a = 1; --error
UPDATE testmeplease SET b = 'I should fail'; --error

ALTER TABLE testmeplease ADD TABLE sub1 AS PARTITION BETWEEN -100 AND 100;

UPDATE testmeplease SET a = 99 WHERE a = 2;
UPDATE testmeplease SET b = 'I should fail again'; --error

INSERT INTO testmeplease VALUES (1, '1'), (2, '2'), (3, '3');
INSERT INTO testmeplease VALUES (1, 'cannot cast me'); --error
INSERT INTO sub1 VALUES (1, 'cannot cast me'); --error

UPDATE testmeplease SET a = 150 WHERE a = 3;
UPDATE testmeplease SET b = 'Cannot update me' WHERE a = 3; --error

UPDATE sub1 SET b = 'Cannot update me'; --error
UPDATE sub1 SET a = 30, b = '30' WHERE a = 1; --error
UPDATE sub1 SET a = 50 WHERE a = 1;

SELECT a, b FROM testmeplease;
SELECT a, b FROM sub1;

ALTER TABLE testmeplease DROP TABLE sub1;

DROP TABLE sub1;
DROP TABLE testmeplease;
DROP FUNCTION casttoint;
