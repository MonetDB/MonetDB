--
-- VACUUM
--
-- Replaced PostgreSQL "VACUUM FULL my_table;"
-- with MonetDB "call vacuum(current_schema, 'my_table');"
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

select "schema", "table", "column", type, count, typewidth, columnsize, heapsize, hashes, "imprints", sorted from sys.storage(current_schema, 'vactst');
call vacuum(current_schema, 'vactst');
select "schema", "table", "column", type, count, typewidth, columnsize, heapsize, hashes, "imprints", sorted from sys.storage(current_schema, 'vactst');

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

select "schema", "table", "column", type, count, typewidth, columnsize, heapsize, hashes, "imprints", sorted from sys.storage(current_schema, 'vactst');
call vacuum(current_schema, 'vactst');
select "schema", "table", "column", type, count, typewidth, columnsize, heapsize, hashes, "imprints", sorted from sys.storage(current_schema, 'vactst');

DELETE FROM vactst;
SELECT * FROM vactst;

DROP TABLE vactst;
