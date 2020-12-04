START TRANSACTION;

CREATE FUNCTION SQ (s STRING) RETURNS STRING BEGIN RETURN ' ''' || s || ''' '; END;
CREATE FUNCTION DQ (s STRING) RETURNS STRING BEGIN RETURN '"' || s || '"'; END; --TODO: Figure out why this breaks with the space

CREATE FUNCTION comment_on(ob STRING, id STRING, r STRING) RETURNS STRING BEGIN RETURN ifthenelse(r IS NOT NULL, 'COMMENT ON ' || ob ||  ' ' || id || ' IS ' || SQ(r) || ';', ''); END;

CREATE FUNCTION dump_type(type STRING, digits INT, scale INT) RETURNS STRING BEGIN
	RETURN
		CASE
		WHEN type = 'boolean' THEN  'BOOLEAN'
		WHEN type = 'int' THEN  'INTEGER'
		WHEN type = 'smallint' THEN  'SMALLINT'
		WHEN type = 'tinyiny' THEN  'TINYINT'
		WHEN type = 'bigint' THEN  'BIGINT'
		WHEN type = 'hugeint' THEN  'HUGEINT'
		WHEN type = 'date' THEN  'DATE'
		WHEN type = 'month_interval' THEN  CASE
			WHEN digits = 1 THEN 'INTERVAL YEAR'
			WHEN digits = 2 THEN 'INTERVAL YEAR TO MONTH'
			ELSE  'INTERVAL MONTH' --ASSUMES digits = 3
			END
		WHEN type LIKE '%_INTERVAL' THEN  CASE
			WHEN digits = 4  THEN 'INTERVAL DAY'
			WHEN digits = 5  THEN 'INTERVAL DAY TO HOUR'
			WHEN digits = 6  THEN 'INTERVAL DAY TO MINUTE'
			WHEN digits = 7  THEN 'INTERVAL DAY TO SECOND'
			WHEN digits = 8  THEN 'INTERVAL HOUR'
			WHEN digits = 9  THEN 'INTERVAL HOUR TO MINUTE'
			WHEN digits = 10 THEN 'INTERVAL HOUR TO SECOND'
			WHEN digits = 11 THEN 'INTERVAL MINUTE'
			WHEN digits = 12 THEN 'INTERVAL MINUTE TO SECOND'
			ELSE  'INTERVAL SECOND' --ASSUMES digits = 13
			END
		WHEN type = 'varchar' OR type = 'clob' THEN  CASE
			WHEN digits = 0 THEN 'CHARACTER LARGE OBJECT'
			ELSE 'CHARACTER LARGE OBJECT(' || digits || ')' --ASSUMES digits IS NOT NULL
			END
		WHEN type = 'blob' THEN  CASE
			WHEN digits = 0 THEN 'BINARY LARGE OBJECT'
			ELSE 'BINARY LARGE OBJECT(' || digits || ')' --ASSUMES digits IS NOT NULL
			END
		WHEN type = 'timestamp'  THEN 'TIMESTAMP' || ifthenelse(digits <> 7, '(' || (digits -1) || ') ', ' ')
		WHEN type = 'timestamptz' THEN 'TIMESTAMP' || ifthenelse(digits <> 7, '(' || (digits -1) || ') ', ' ') || 'WITH TIME ZONE'
		WHEN type = 'time'  THEN 'TIME' || ifthenelse(digits <> 1, '(' || (digits -1) || ') ', ' ')
		WHEN type = 'timetz' THEN 'TIME' || ifthenelse(digits <> 1, '(' || (digits -1) || ') ', ' ') || 'WITH TIME ZONE'
		WHEN type = 'real' THEN CASE
			WHEN digits = 24 AND scale=0 THEN 'REAL'
			WHEN scale=0 THEN 'FLOAT(' || digits || ')'
			ELSE 'FLOAT(' || digits || ',' || scale || ')'
			END
		WHEN type = 'double' THEN CASE
			WHEN digits = 53 AND scale=0 THEN 'DOUBLE'
			WHEN scale = 0 THEN 'FLOAT(' || digits || ')'
			ELSE 'FLOAT(' || digits || ',' || scale || ')'
			END
		WHEN type = 'decimal' THEN CASE
			WHEN (digits = 1 AND scale = 0) OR digits = 0 THEN 'DECIMAL'
			WHEN scale = 0 THEN 'DECIMAL(' || digits || ')'
			WHEN digits = 39 THEN 'DECIMAL(' || 38 || ',' || scale || ')'
			WHEN digits = 19 AND (SELECT COUNT(*) = 0 FROM sys.types WHERE sqlname = 'hugeint' ) THEN 'DECIMAL(' || 18 || ',' || scale || ')'
			ELSE 'DECIMAL(' || digits || ',' || scale || ')'
			END
		ELSE upper(type) || '(' || digits || ',' || scale || ')' --TODO: might be a bit too simple
		END;
END;

CREATE FUNCTION dump_CONSTRAINT_type_name(id INT) RETURNS STRING BEGIN
	RETURN
		CASE
		WHEN id = 0 THEN 'PRIMARY KEY'
		WHEN id = 1 THEN 'UNIQUE'
		END;
END;

CREATE FUNCTION describe_constraints() RETURNS TABLE("table" STRING, nr INT, col STRING, con STRING, type STRING) BEGIN
	RETURN
		SELECT t.name, kc.nr, kc.name, k.name, dump_CONSTRAINT_type_name(k.type)
		FROM sys._tables t, sys.objects kc, sys.keys k
		WHERE kc.id = k.id
			AND k.table_id = t.id
			AND t.system = FALSE
			AND k.type in (0, 1)
			AND t.type IN (0, 6);
END;

CREATE FUNCTION dump_table_constraint_type() RETURNS TABLE(stm STRING) BEGIN
	RETURN
		SELECT 
			'ALTER TABLE ' || DQ("table") ||
			' ADD CONSTRAINT ' || DQ(con) || ' '||
			type || ' (' || GROUP_CONCAT(DQ(col), ', ') || ');'
		FROM describe_constraints() GROUP BY "table", con, type;
END;

CREATE FUNCTION describe_indices() RETURNS TABLE (i STRING, o INT, s STRING, t STRING, c STRING, it STRING) BEGIN
RETURN
	WITH it (id, idx) AS (VALUES (0, 'INDEX'), (4, 'IMPRINTS INDEX'), (5, 'ORDERED INDEX')) --UNIQUE INDEX wraps to INDEX.
	SELECT
		i.name,
		kc.nr, --TODO: Does this determine the concatenation order?
		s.name,
		t.name,
		c.name,
		it.idx
	FROM
		sys.idxs AS i LEFT JOIN sys.keys AS k ON i.name = k.name,
		sys.objects AS kc,
		sys._columns AS c,
		sys.schemas s,
		sys._tables AS t,
		it
	WHERE
		i.table_id = t.id
		AND i.id = kc.id
		AND kc.name = c.name
		AND t.id = c.table_id
		AND t.schema_id = s.id
		AND k.type IS NULL
		AND i.type = it.id
	ORDER BY i.name, kc.nr;
END;

CREATE FUNCTION dump_indices() RETURNS TABLE(stm STRING) BEGIN
	RETURN
		SELECT
			'CREATE ' || it || ' ' ||
			DQ(i) || ' ON ' || DQ(s) || '.' || DQ(t) ||
			'(' || GROUP_CONCAT(c) || ');'
		FROM describe_indices() GROUP BY i, it, s, t;
END;

CREATE FUNCTION dump_column_definition(tid INT) RETURNS STRING BEGIN
	RETURN
		SELECT 
			' (' ||
			GROUP_CONCAT(
				DQ(c.name) || ' ' ||
				dump_type(c.type, c.type_digits, c.type_scale) ||
				ifthenelse(c."null" = 'false', ' NOT NULL', '') ||
				ifthenelse(c."default" IS NOT NULL, ' DEFAULT ' || c."default", '')
			, ', ') || ')'
		FROM sys._columns c 
		WHERE c.table_id = tid;
END;

CREATE FUNCTION dump_remote_table_expressions(s STRING, t STRING) RETURNS STRING BEGIN
	RETURN SELECT ' ON ' || SQ(uri) || ' WITH USER ' || SQ(username) || ' ENCRYPTED PASSWORD ' || SQ("hash") FROM sys.remote_table_credentials(s ||'.' || t);
END;

CREATE FUNCTION dump_merge_table_partition_expressions(tid INT) RETURNS STRING
BEGIN
	RETURN SELECT
			' PARTITION BY ' ||
			CASE
				WHEN bit_and(tp.type, 2) = 2
				THEN 'VALUES '
               	ELSE 'RANGE '
			END ||
			CASE
				WHEN bit_and(tp.type, 4) = 4 --column expression
				THEN 'ON ' || '(' || (SELECT DQ(c.name) || ')' FROM sys.columns c WHERE c.id = tp.column_id)
				ELSE 'USING ' || '(' || tp.expression || ')' --generic expression
			END
	FROM sys.table_partitions tp
	WHERE tp.table_id = tid;
END;

--SELECT * FROM dump_foreign_keys();
CREATE FUNCTION describe_foreign_keys() RETURNS TABLE(
	fk_s STRING, fk_t STRING, fk_c STRING,
	o INT, fk STRING,
	pk_s STRING, pk_t STRING, pk_c STRING,
	on_update STRING, on_delete STRING) BEGIN

	RETURN 
		WITH action_type (id, act) AS (VALUES 
			(0, 'NO ACTION'),
			(1, 'CASCADE'),
			(2, 'RESTRICT'),
			(3, 'SET NULL'),
			(4, 'SET DEFAULT'))
		SELECT
		fs.name AS fsname, fkt.name AS ktname, fkkc.name AS fcname,
		fkkc.nr AS o, fkk.name AS fkname,
		ps.name AS psname, pkt.name AS ptname, pkkc.name AS pcname,
		ou.act as on_update, od.act as on_delete
					FROM sys._tables fkt,
						sys.objects fkkc,
						sys.keys fkk,
						sys._tables pkt,
						sys.objects pkkc,
						sys.keys pkk,
						sys.schemas ps,
						sys.schemas fs,
						action_type ou,
						action_type od
		
					WHERE fkt.id = fkk.table_id
					AND pkt.id = pkk.table_id
					AND fkk.id = fkkc.id
					AND pkk.id = pkkc.id
					AND fkk.rkey = pkk.id
					AND fkkc.nr = pkkc.nr
					AND pkt.schema_id = ps.id
					AND fkt.schema_id = fs.id
					AND (fkk."action" & 255)         = ou.id
					AND ((fkk."action" >> 8) & 255)  = od.id
					ORDER BY fkk.name, fkkc.nr;
END;

CREATE FUNCTION dump_foreign_keys() RETURNS TABLE(stmt STRING) BEGIN
RETURN
	SELECT 
		'ALTER TABLE ' || DQ(fk_s) || '.'|| DQ(fk_t) || ' ADD CONSTRAINT ' || DQ(fk) || ' ' ||
		'FOREIGN KEY(' || GROUP_CONCAT(DQ(fk_c), ',') ||') ' ||
		'REFERENCES ' || DQ(pk_s) || '.' || DQ(pk_t) || '(' || GROUP_CONCAT(DQ(pk_c), ',') || ') ' ||
		'ON DELETE ' || on_delete || ' ON UPDATE ' || on_update
	FROM describe_foreign_keys() GROUP BY fk_s, fk_t, pk_s, pk_t, fk, on_delete, on_update;
END;

CREATE TEMPORARY TABLE dump_statements(o INT AUTO_INCREMENT, s STRING, PRIMARY KEY (o));

CREATE PROCEDURE dump_database(describe BOOLEAN)
BEGIN

    set schema sys;

	INSERT INTO dump_statements(s) VALUES ('START TRANSACTION;');

	INSERT INTO dump_statements(s) --dump_create_roles
		SELECT 'CREATE ROLE ' || DQ(name) || ';' FROM auths
        WHERE name NOT IN (SELECT name FROM db_user_info) 
        AND grantor <> 0;

	INSERT INTO dump_statements(s) --dump_create_users
		SELECT
        'CREATE USER ' ||  DQ(ui.name) ||  ' WITH ENCRYPTED PASSWORD ' ||
            SQ(password_hash(ui.name)) ||
        ' NAME ' || SQ(ui.fullname) ||  ' SCHEMA sys;'
        FROM db_user_info ui, schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot';

	INSERT INTO dump_statements(s) --dump_create_schemas
        SELECT
            'CREATE SCHEMA ' ||  DQ(s.name) || ifthenelse(a.name <> 'sysadmin', ' AUTHORIZATION ' || a.name, ' ') || ';'
        FROM schemas s, auths a
        WHERE s.authorization = a.id AND s.system = FALSE;

	INSERT INTO dump_statements(s) --dump_create_comments_on_schemas
        SELECT comment_on('SCHEMA', DQ(s.name), rem.remark)
        FROM schemas s JOIN comments rem ON s.id = rem.id
        WHERE NOT s.system;

    INSERT INTO dump_statements(s) --dump_add_schemas_to_users
	    SELECT
            'ALTER USER ' || DQ(ui.name) || ' SET SCHEMA ' || DQ(s.name) || ';'
        FROM db_user_info ui, schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot'
            AND s.name <> 'sys';

    INSERT INTO dump_statements(s) --dump_grant_user_priviledges
        SELECT
            'GRANT ' || DQ(a2.name) || ' ' || ifthenelse(a1.name = 'public', 'PUBLIC', DQ(a1.name)) || ';'
		FROM sys.auths a1, sys.auths a2, sys.user_role ur
		WHERE a1.id = ur.login_id AND a2.id = ur.role_id;

	INSERT INTO dump_statements(s) --dump_create_sequences
		SELECT 'CREATE SEQUENCE ' || DQ(sch.name) || '.' || DQ(seq.name) || ' AS INTEGER;'
		FROM sys.schemas sch, sys.sequences seq
		WHERE sch.id = seq.schema_id;

	INSERT INTO dump_statements(s) --dump_create_comments_on_sequences
        SELECT comment_on('SEQUENCE', DQ(sch.name) || '.' || DQ(seq.name), rem.remark)
        FROM
			sys.schemas sch,
			sys.sequences seq JOIN sys.comments rem ON seq.id = rem.id
		WHERE sch.id = seq.schema_id;

	INSERT INTO dump_statements(s) --dump_create_tables
		SELECT 
			'CREATE ' || ts.table_type_name || ' ' || DQ(s.name) || '.' || DQ(t.name) || dump_column_definition(t.id) || 
			CASE
				WHEN ts.table_type_name = 'REMOTE TABLE' THEN
					dump_remote_table_expressions(s.name, t.name) || ';'
				WHEN ts.table_type_name = 'MERGE TABLE' THEN
					dump_merge_table_partition_expressions(t.id) || ';'
				ELSE
					';'
			END
		FROM sys.schemas s, table_types ts, sys._tables t
		WHERE ts.table_type_name IN ('TABLE', 'MERGE TABLE', 'REMOTE TABLE', 'REPLICA TABLE')
			AND t.system = FALSE
			AND s.id = t.schema_id
			AND ts.table_type_id = t.type
			AND s.name <> 'tmp';

	INSERT INTO dump_statements(s) SELECT * FROM dump_table_constraint_type();
	INSERT INTO dump_statements(s) SELECT * FROM dump_indices();
	INSERT INTO dump_statements(s) SELECT * FROM dump_foreign_keys();

	INSERT INTO dump_statements(s) --dump_create_comments_on_indices
        SELECT comment_on('INDEX', DQ(i.name), rem.remark)
        FROM sys.idxs i JOIN sys.comments rem ON i.id = rem.id;

	INSERT INTO dump_statements(s) --dump_create_comments_on_columns
        SELECT comment_on('COLUMN', DQ(s.name) || '.' || DQ(t.name) || '.' || DQ(c.name), rem.remark)
		FROM sys.columns c JOIN sys.comments rem ON c.id = rem.id, sys.tables t, sys.schemas s WHERE c.table_id = t.id AND t.schema_id = s.id AND NOT t.system;

	--INSERT INTO dump_statements(s) SELECT * FROM dump_foreign_keys();

	--TODO ADD schema's to ALTER statements
	--TODO PARTITION TABLES
	--TODO details on SEQUENCES
	--TODO functions
	--TODO Triggers
	--TODO VIEW
	--TODO COMMENTS ON TABLE
	--TODO TABLE level grants
	--TODO COLUMN level grants
	--TODO User Defined Types? sys.types
	--TODO STREAM TABLE?
	--TODO Triggers

    INSERT INTO dump_statements(s) VALUES ('COMMIT;');

END;

CALL dump_database(TRUE);
SELECT GROUP_CONCAT(s) OVER (PARTITION BY o range between current row and current row) FROM dump_statements;

ROLLBACK;
