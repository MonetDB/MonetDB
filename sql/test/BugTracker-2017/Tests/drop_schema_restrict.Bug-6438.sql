CREATE SCHEMA xyz;
CREATE TABLE xyz.t1 (col1 int Primary Key);
CREATE VIEW xyz.v1 as SELECT col1 FROM xyz.t1 WHERE col1 > 0 ORDER BY col1;
INSERT INTO xyz.t1 VALUES (0), (2), (1);

SELECT * FROM xyz.t1;
SELECT * FROM xyz.v1;

DROP SCHEMA xyz RESTRICT;
-- this should return an error as there are objects (table, column, pkey, view) depending on the schema
SELECT * FROM xyz.t1;
SELECT * FROM xyz.v1;

DROP SCHEMA xyz;
-- this should return an error as the default behavior should be RESTRICT and there are objects (table, column, pkey, view) depending on the schema
SELECT * FROM xyz.t1;
SELECT * FROM xyz.v1;

DROP SCHEMA xyz CASCADE;
-- this should return success. Also all depending objects should be dropped
SELECT * FROM xyz.t1;
SELECT * FROM xyz.v1;

DROP SCHEMA xyz CASCADE;
-- this should return an error as the schema should not exist anymore

DROP SCHEMA IF EXISTS xyz CASCADE;
-- this should return success.

CREATE SCHEMA xyz2;
CREATE TABLE xyz2.t1 (col1 int Primary Key);
CREATE VIEW xyz2.v1 as SELECT col1 FROM xyz2.t1 WHERE col1 > 0 ORDER BY col1;
INSERT INTO xyz2.t1 VALUES (0), (2), (1);

DROP SCHEMA xyz2 RESTRICT;
-- this should return an error as there are objects (table, column, pkey, view) depending on the schema
SELECT * FROM xyz2.t1;
SELECT * FROM xyz2.v1;

DROP TABLE xyz2.t1 CASCADE;
-- this should drop the table and the dependent view
SELECT * FROM xyz2.t1;
SELECT * FROM xyz2.v1;

DROP SCHEMA xyz2 RESTRICT;
-- this should return success as there are no depending objects anymore

DROP SCHEMA IF EXISTS xyz2 CASCADE;
-- this should return success.

