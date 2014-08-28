--
-- VACUUM
--
-- There is no VACUUM statement in the SQL standard.
--

CREATE TABLE vactst (i INT);
INSERT INTO vactst VALUES (1);
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
SELECT i, count(*) AS count FROM vactst GROUP BY i ORDER BY i;
SELECT count(*) FROM vactst WHERE i <> 0;
DELETE FROM vactst WHERE i <> 0;
SELECT * FROM vactst;
/* VACUUM FULL vactst; */
UPDATE vactst SET i = i + 1;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
SELECT i, count(*) AS count FROM vactst GROUP BY i ORDER BY i;
SELECT count(*) FROM vactst WHERE i <> 0;
DELETE FROM vactst WHERE i <> 0;
/* VACUUM FULL vactst; */
DELETE FROM vactst;
SELECT * FROM vactst;

DROP TABLE vactst;
