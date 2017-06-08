# Test DROP VIEW IF EXISTS functionality

CREATE TABLE tab0 (col0 INTEGER, col2 TEXT);
INSERT INTO tab0 VALUES (1,'lekker'), (2, 'heerlijk'), (3, 'smullen');

CREATE VIEW view1 AS SELECT * FROM tab0;
CREATE VIEW view2 AS SELECT COUNT(*) FROM tab0;

DROP VIEW view1;
SELECT * FROM view1; -- should fail
DROP VIEW IF EXISTS view1;

DROP VIEW IF EXISTS view2;
DROP VIEW view2; -- should fail
SELECT * FROM view2; -- should fail

DROP TABLE tab0;
