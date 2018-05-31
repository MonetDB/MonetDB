CREATE FUNCTION dosomething(a int) RETURNS INT BEGIN RETURN a + 2; END;

CREATE MERGE TABLE trydropme (a int, b int, cc varchar(32), dd real) PARTITION BY VALUES ON (cc);

ALTER TABLE trydropme ADD COLUMN failing int; --error
ALTER TABLE trydropme DROP COLUMN b;
ALTER TABLE trydropme DROP COLUMN cc; --error
DROP TABLE trydropme;

CREATE MERGE TABLE nexttest (a int, b int, cc varchar(32), dd real) PARTITION BY VALUES USING (dosomething(a) + dosomething(b));

ALTER TABLE nexttest ADD COLUMN failing int; --error
ALTER TABLE nexttest DROP COLUMN cc;
ALTER TABLE nexttest DROP COLUMN a; --error
DROP FUNCTION dosomething; --error

CREATE TABLE subtable1 (a int, b int, dd real);
CREATE TABLE subtable2 (a int, b int, dd real);

INSERT INTO subtable1 VALUES (4, 2, 1.2);

ALTER TABLE nexttest ADD TABLE subtable1 AS PARTITION IN ('ups'); --error

ALTER TABLE nexttest ADD TABLE subtable1 AS PARTITION IN (1, 2, 10);
ALTER TABLE nexttest ADD TABLE subtable2 AS PARTITION IN (11, 12, 20);

ALTER TABLE nexttest DROP COLUMN dd; --error
ALTER TABLE nexttest ADD COLUMN again int; --error
ALTER TABLE subtable1 DROP COLUMN dd; --error
ALTER TABLE subtable1 ADD COLUMN again int; --error

ALTER TABLE nexttest DROP TABLE subtable1;
ALTER TABLE nexttest DROP TABLE subtable2;

DROP TABLE subtable1;
DROP TABLE subtable2;
DROP TABLE nexttest;
DROP FUNCTION dosomething;

CREATE FUNCTION dosomethingelse(i int) RETURNS TABLE (j int) BEGIN RETURN TABLE(SELECT i); END;

CREATE MERGE TABLE nexttest (a int, dd real) PARTITION BY VALUES USING (dosomethingelse(a)); --error
CREATE TABLE subtable3 (a int, dd real);
INSERT INTO subtable3 VALUES (0, 1.68);

ALTER TABLE nexttest ADD TABLE subtable3 AS PARTITION IN (1, 2, 10); --error

DROP TABLE subtable3;
DROP TABLE nexttest; --error
DROP FUNCTION dosomethingelse;
