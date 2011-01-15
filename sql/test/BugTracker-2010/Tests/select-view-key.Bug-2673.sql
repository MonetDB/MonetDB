CREATE TABLE t2673 (
	name varchar(128) NOT NULL,
	field varchar(128) NOT NULL,
	PRIMARY KEY (field, name)
);
CREATE VIEW v2673 AS SELECT name FROM t2673  WHERE field='SpecLineNames';
SELECT
        v2673.name
FROM
        v2673
WHERE
        v2673.name <> 'UNKNOWN'
        and v2673.name = 'Hb_4863'
;

DROP VIEW v2673;
DROP TABLE t2673;
