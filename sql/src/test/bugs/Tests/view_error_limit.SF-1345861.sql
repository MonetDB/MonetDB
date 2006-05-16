START TRANSACTION;

-- these used to generate an error about LIMIT not allowed

-- view based on DVDstore 'hack' reported by Stefan
CREATE VIEW view1345861 AS
	SELECT * FROM sys._tables
	UNION ALL
	SELECT * FROM tmp._tables;
DROP VIEW view1345861;

-- this view is an artifical example based on what provided in bug #1345861
CREATE VIEW view1345861 AS
	SELECT tables.name FROM tables INNER JOIN columns
		ON tables.id = columns.table_id
		WHERE columns.name LIKE 'n%'
	INTERSECT
	SELECT tables.name FROM tables INNER JOIN columns
		ON tables.id = columns.table_id
		WHERE columns.name LIKE 's%';
DROP VIEW view1345861;

-- clean up, if anything to clean up
ROLLBACK;
