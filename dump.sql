START TRANSACTION;

--We start with creating static versions of catalogue tables that are going to be affected by this dump script itself.
CREATE TEMPORARY TABLE _user_sequences AS SELECT * FROM sys.sequences;
CREATE TEMPORARY TABLE _user_functions AS SELECT * FROM sys.functions f WHERE NOT f.system;

CREATE FUNCTION SQ (s STRING) RETURNS STRING BEGIN RETURN ' ''' || s || ''' '; END;
CREATE FUNCTION DQ (s STRING) RETURNS STRING BEGIN RETURN '"' || s || '"'; END; --TODO: Figure out why this breaks with the space
CREATE FUNCTION FQTN(s STRING, t STRING) RETURNS STRING BEGIN RETURN DQ(s) || '.' || DQ(t); END;
CREATE FUNCTION ALTER_TABLE(s STRING, t STRING) RETURNS STRING BEGIN RETURN 'ALTER TABLE ' || FQTN(s, t) || ' '; END;

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

CREATE FUNCTION describe_constraints() RETURNS TABLE(s STRING, "table" STRING, nr INT, col STRING, con STRING, type STRING) BEGIN
	RETURN
		SELECT s.name, t.name, kc.nr, kc.name, k.name, dump_CONSTRAINT_type_name(k.type)
		FROM sys.schemas s, sys._tables t, sys.objects kc, sys.keys k
		WHERE kc.id = k.id
			AND k.table_id = t.id
			AND s.id = t.schema_id
			AND t.system = FALSE
			AND k.type in (0, 1)
			AND t.type IN (0, 6);
END;

CREATE FUNCTION dump_table_constraint_type() RETURNS TABLE(stm STRING) BEGIN
	RETURN
		SELECT
			'ALTER TABLE ' || DQ(s) || '.' || DQ("table") ||
			' ADD CONSTRAINT ' || DQ(con) || ' '||
			type || ' (' || GROUP_CONCAT(DQ(col), ', ') || ');'
		FROM describe_constraints() GROUP BY s, "table", con, type;
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
	RETURN
		SELECT
			CASE WHEN tp.table_id IS NOT NULL THEN	--updatable merge table
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
			ELSE									--read only partition merge table.
				''
			END
		FROM (VALUES (tid)) t(id) LEFT JOIN sys.table_partitions tp ON t.id = tp.table_id;
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
		'ON DELETE ' || on_delete || ' ON UPDATE ' || on_update ||
		';'
	FROM describe_foreign_keys() GROUP BY fk_s, fk_t, pk_s, pk_t, fk, on_delete, on_update;
END;

CREATE FUNCTION describe_partition_tables()
RETURNS TABLE(
	m_sname STRING,
	m_tname STRING,
	p_sname STRING,
	p_tname STRING,
	p_type  STRING,
	pvalues STRING,
	minimum STRING,
	maximum STRING,
	with_nulls BOOLEAN) BEGIN
RETURN
  SELECT 
        m_sname,
        m_tname,
        p_sname,
        p_tname,
        CASE
            WHEN p_raw_type IS NULL THEN 'READ ONLY'
            WHEN (p_raw_type = 'VALUES' AND pvalues IS NULL) OR (p_raw_type = 'RANGE' AND minimum IS NULL AND maximum IS NULL AND with_nulls) THEN 'FOR NULLS'
            ELSE p_raw_type
        END AS p_type,
        pvalues,
        minimum,
        maximum,
        with_nulls
    FROM 
    (WITH
		tp("type", table_id) AS
		(SELECT CASE WHEN (table_partitions."type" & 2) = 2 THEN 'VALUES' ELSE 'RANGE' END, table_partitions.table_id FROM table_partitions),
		subq(m_tid, p_mid, "type", m_sname, m_tname, p_sname, p_tname) AS
		(SELECT m_t.id, p_m.id, m_t."type", m_s.name, m_t.name, p_s.name, p_m.name
		FROM schemas m_s, sys._tables m_t, dependencies d, schemas p_s, sys._tables p_m
		WHERE m_t."type" IN (3, 6)
			AND m_t.schema_id = m_s.id
			AND m_s.name <> 'tmp'
			AND m_t.system = FALSE
			AND m_t.id = d.depend_id
			AND d.id = p_m.id
			AND p_m.schema_id = p_s.id
		ORDER BY m_t.id, p_m.id)
	SELECT
		subq.m_sname,
		subq.m_tname,
		subq.p_sname,
		subq.p_tname,
		tp."type" AS p_raw_type,
		CASE WHEN tp."type" = 'VALUES'
			THEN (SELECT GROUP_CONCAT(vp.value, ',')FROM value_partitions vp WHERE vp.table_id = subq.p_mid)
			ELSE NULL
		END AS pvalues,
		CASE WHEN tp."type" = 'RANGE'
			THEN (SELECT minimum FROM range_partitions rp WHERE rp.table_id = subq.p_mid)
			ELSE NULL
		END AS minimum,
		CASE WHEN tp."type" = 'RANGE'
			THEN (SELECT maximum FROM range_partitions rp WHERE rp.table_id = subq.p_mid)
			ELSE NULL
		END AS maximum,
		CASE WHEN tp."type" = 'VALUES'
			THEN EXISTS(SELECT vp.value FROM value_partitions vp WHERE vp.table_id = subq.p_mid AND vp.value IS NULL)
			ELSE (SELECT rp.with_nulls FROM range_partitions rp WHERE rp.table_id = subq.p_mid)
		END AS with_nulls
	FROM 
		subq LEFT OUTER JOIN tp
		ON subq.m_tid = tp.table_id) AS tmp_pi;
END;

CREATE FUNCTION dump_partition_tables() RETURNS TABLE(stmt STRING) BEGIN
RETURN
	SELECT
		ALTER_TABLE(m_sname, m_tname) || ' ADD TABLE ' || FQTN(p_sname, p_tname) || 
		CASE 
			WHEN p_type = 'VALUES' THEN ' AS PARTITION IN (' || pvalues || ')'
			WHEN p_type = 'RANGE' THEN ' AS PARTITION FROM ' || ifthenelse(minimum IS NOT NULL, SQ(minimum), 'RANGE MINVALUE') || ' TO ' || ifthenelse(maximum IS NOT NULL, SQ(maximum), 'RANGE MAXVALUE')
			WHEN p_type = 'FOR NULLS' THEN ' AS PARTITION FOR NULL VALUES'
			ELSE '' --'READ ONLY'
		END ||
		CASE WHEN p_type in ('VALUES', 'RANGE') AND with_nulls THEN ' WITH NULL VALUES' ELSE '' END ||
		';' 
	FROM describe_partition_tables();
END;

CREATE FUNCTION describe_sequences()
	RETURNS TABLE(
		sch STRING,
		seq STRING,
		s BIGINT,
		rs BIGINT,
		mi BIGINT,
		ma BIGINT,
		inc BIGINT,
		cache BIGINT,
		cycle BOOLEAN)
BEGIN
	RETURN SELECT
	s.name as sch,
	seq.name as seq,
	seq."start",
	get_value_for(s.name, seq.name) AS "restart",
	seq."minvalue",
	seq."maxvalue",
	seq."increment",
	seq."cacheinc",
	seq."cycle"
	FROM _user_sequences seq, sys.schemas s
	WHERE s.id = seq.schema_id
	ORDER BY s.name, seq.name;
END;

CREATE FUNCTION dump_sequences() RETURNS TABLE(stmt STRING) BEGIN
RETURN
	SELECT
		'CREATE SEQUENCE ' || FQTN(sch, seq) || ' AS BIGINT ' ||
		CASE WHEN "s" <> 0 THEN ' START WITH ' || "s" ELSE '' END ||
		CASE WHEN "inc" <> 1 THEN ' INCREMENT BY ' || "inc" ELSE '' END ||
		CASE WHEN "mi" <> 0 THEN ' MINVALUE ' || "mi" ELSE '' END ||
		CASE WHEN "ma" <> 0 THEN ' MAXVALUE ' || "ma" ELSE '' END ||
		CASE WHEN "cache" <> 1 THEN ' CACHE ' || "cache" ELSE '' END ||
		CASE WHEN "cycle" THEN ' CYCLE' ELSE '' END || ';'
	FROM describe_sequences();
END;

CREATE FUNCTION describe_functions() RETURNS TABLE (o INT, sch STRING, fun STRING, def STRING) BEGIN
RETURN
	SELECT f.id, s.name, f.name, f.func from _user_functions f JOIN schemas s ON f.schema_id = s.id;
END;

CREATE FUNCTION dump_functions() RETURNS TABLE (o INT, stmt STRING) BEGIN
	RETURN SELECT f.o, 'SET SCHEMA ' || DQ(f.sch) || ';' || f.def || 'SET SCHEMA "sys";' FROM describe_functions() f;
END;

CREATE FUNCTION describe_tables() RETURNS TABLE(o INT, sch STRING, tab STRING, typ STRING,  col STRING, opt STRING) BEGIN
RETURN
	SELECT
		t.id,
		s.name,
		t.name,
		ts.table_type_name,
		dump_column_definition(t.id),
		CASE
			WHEN ts.table_type_name = 'REMOTE TABLE' THEN
				dump_remote_table_expressions(s.name, t.name)
			WHEN ts.table_type_name = 'MERGE TABLE' THEN
				dump_merge_table_partition_expressions(t.id)
			WHEN ts.table_type_name = 'VIEW' THEN
				t.query
			ELSE
				''
		END
	FROM sys.schemas s, table_types ts, sys.tables t
	WHERE ts.table_type_name IN ('TABLE', 'VIEW', 'MERGE TABLE', 'REMOTE TABLE', 'REPLICA TABLE')
		AND t.system = FALSE
		AND s.id = t.schema_id
		AND ts.table_type_id = t.type
		AND s.name <> 'tmp';
END;

CREATE FUNCTION dump_tables() RETURNS TABLE (o INT, stmt STRING) BEGIN
RETURN
	SELECT
		t.o,
		CASE
			WHEN t.typ <> 'VIEW' THEN
				'CREATE ' || t.typ || ' ' || FQTN(t.sch, t.tab) || t.col || t.opt || ';'
			ELSE
				t.opt
		END
	FROM describe_tables() t;
END;

--The dump statement should normally have an auto-incremented column representing the creation order.
--But in cases of db objects that can be interdependent, i.e. functions and table-likes, we need access to the underlying sequence of the AUTO_INCREMENT property.
--Because we need to explicitly overwrite the creation order column "o" in those cases. After inserting the dump statements for functions and table-likes,
--we can restart the auto-increment sequence with a sensible value for following dump statements.

CREATE SEQUENCE tmp._auto_increment;
CREATE TEMPORARY TABLE dump_statements(o INT DEFAULT NEXT VALUE FOR tmp._auto_increment, s STRING, PRIMARY KEY (o));

--Because ALTER SEQUENCE statements are not allowed in procedures,
--we have to do a really nasty hack to restart the _auto_increment sequence.

CREATE FUNCTION restart_sequence(sch STRING, seq STRING, val BIGINT) RETURNS BIGINT EXTERNAL NAME sql."restart";

CREATE PROCEDURE dump_database(describe BOOLEAN)
BEGIN

    set schema sys;

	INSERT INTO dump_statements(s) VALUES ('START TRANSACTION;');
	INSERT INTO dump_statements(s) VALUES ('SET SCHEMA "sys";');

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

	INSERT INTO dump_statements(s) SELECT * FROM dump_sequences();

	INSERT INTO dump_statements(s) --dump_create_comments_on_sequences
        SELECT comment_on('SEQUENCE', DQ(sch.name) || '.' || DQ(seq.name), rem.remark)
        FROM
			sys.schemas sch,
			sys.sequences seq JOIN sys.comments rem ON seq.id = rem.id
		WHERE sch.id = seq.schema_id;

	--START OF COMPLICATED DEPENDENCY STUFF:
	--functions and table-likes can be interdependent. They should be inserted in the order of their catalogue id.
	DECLARE offs INT;
	SET offs = (SELECT max(o) FROM dump_statements) - (SELECT min(ids.id) FROM (select id from tables union select id from functions) ids(id));

	INSERT INTO dump_statements SELECT f.o + offs, f.stmt FROM dump_functions() f;
	INSERT INTO dump_statements SELECT t.o + offs, t.stmt FROM dump_tables() t;

	SET offs = (SELECT max(o) + 1 FROM dump_statements);
	DECLARE dummy_result BIGINT; --HACK: otherwise I cannot call restart_sequence.
	SET dummy_result = restart_sequence('tmp', '_auto_increment', offs);
	--END OF COMPLICATED DEPENDENCY STUFF.

	INSERT INTO dump_statements(s) SELECT * FROM dump_table_constraint_type();
	INSERT INTO dump_statements(s) SELECT * FROM dump_indices();
	INSERT INTO dump_statements(s) SELECT * FROM dump_foreign_keys();
	INSERT INTO dump_statements(s) SELECT * FROM dump_partition_tables();

	INSERT INTO dump_statements(s) --dump_create_comments_on_indices
        SELECT comment_on('INDEX', DQ(i.name), rem.remark)
        FROM sys.idxs i JOIN sys.comments rem ON i.id = rem.id;

	INSERT INTO dump_statements(s) --dump_create_comments_on_columns
        SELECT comment_on('COLUMN', DQ(s.name) || '.' || DQ(t.name) || '.' || DQ(c.name), rem.remark)
		FROM sys.columns c JOIN sys.comments rem ON c.id = rem.id, sys.tables t, sys.schemas s WHERE c.table_id = t.id AND t.schema_id = s.id AND NOT t.system;

	--TODO VIEW
	--TODO SCHEMA GUARD
	--TODO Triggers
	--TODO COMMENTS ON TABLE
	--TODO TABLE level grants
	--TODO COLUMN level grants
	--TODO User Defined Types? sys.types
	--TODO ALTER SEQUENCE using RESTART WITH after importing table_data.

    INSERT INTO dump_statements(s) VALUES ('COMMIT;');

END;

CALL dump_database(TRUE);

SELECT s FROM dump_statements order by o;

ROLLBACK;
