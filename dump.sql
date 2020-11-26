START TRANSACTION;

CREATE FUNCTION SQ (s STRING) RETURNS STRING BEGIN RETURN ' ''' || s || ''' '; END;
CREATE FUNCTION DQ (s STRING) RETURNS STRING BEGIN RETURN '"' || s || '"'; END; --TODO: Figure out why this breaks with the space
CREATE FUNCTION I (s STRING) RETURNS STRING BEGIN RETURN '\t' || s || '\n'; END;
CREATE FUNCTION ENI (s STRING) RETURNS STRING BEGIN RETURN I(SQ(s)); END;

CREATE FUNCTION comment_on(ob STRING, id STRING, r STRING) RETURNS STRING BEGIN RETURN ifthenelse(r IS NOT NULL, '\nCOMMENT ON ' || ob ||  ' ' || id || ' IS ' || SQ(r) || ';', ''); END;

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
		WHEN type = 'timestampz' THEN 'TIMESTAMP' || ifthenelse(digits <> 7, '(' || (digits -1) || ') ', ' ') || 'WITH TIME ZONE'
		WHEN type = 'time'  THEN 'TIME' || ifthenelse(digits <> 1, '(' || (digits -1) || ') ', ' ')
		WHEN type = 'timez' THEN 'TIME' || ifthenelse(digits <> 1, '(' || (digits -1) || ') ', ' ') || 'WITH TIME ZONE'
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

--TODO expand dump_column_definition functionality
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

CREATE FUNCTION dump_contraint_type_name(id INT) RETURNS STRING BEGIN
	RETURN
		CASE
		WHEN id = 0 THEN 'PRIMARY KEY'
		WHEN id = 1 THEN 'UNIQUE'
		END;
END;

CREATE FUNCTION describe_constraints() RETURNS TABLE("table" STRING, nr INT, col STRING, con STRING, type STRING) BEGIN
	RETURN
		SELECT t.name, kc.nr, kc.name, k.name, dump_contraint_type_name(k.type)
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
			ifthenelse(con IS NOT NULL, ' ADD CONTRAINT ' || DQ(con), '') || ' '||
			type || ' (' || GROUP_CONCAT(DQ(col), ', ') || ');'
		FROM describe_constraints() GROUP BY "table", con, type;
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
        'CREATE USER ' ||  DQ(ui.name) ||  ' WITH ENCRYPTED PASSWORD\n' ||
            ENI(password_hash(ui.name)) ||
        'NAME ' || ui.fullname ||  ' SCHEMA sys;'
        FROM db_user_info ui, schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot';

	INSERT INTO dump_statements(s) --dump_create_schemas
        SELECT
            'CREATE SCHEMA ' ||  DQ(s.name) || ifthenelse(a.name <> 'sysadmin', ' AUTHORIZATION ' || a.name, ' ') || ';' ||
            comment_on('SCHEMA', DQ(s.name), rem.remark)
        FROM schemas s LEFT OUTER JOIN comments rem ON s.id = rem.id,auths a
        WHERE s.authorization = a.id AND s.system = FALSE;

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
		SELECT 'CREATE SEQUENCE ' || DQ(sch.name) || '.' || DQ(seq.name) || ' AS INTEGER;' ||
		comment_on('SEQUENCE', DQ(sch.name) || '.' || DQ(seq.name), rem.remark)
		FROM sys.schemas sch,
			sys.sequences seq LEFT OUTER JOIN sys.comments rem ON seq.id = rem.id
		WHERE sch.id = seq.schema_id;

	INSERT INTO dump_statements(s) --dump_create_tables
		SELECT 'CREATE ' || ts.table_type_name || ' ' || DQ(s.name) || '.' || DQ(t.name) || dump_column_definition(t.id) || ';'
		FROM sys.schemas s, table_types ts, sys._tables t LEFT OUTER JOIN sys.comments rem ON t.id = rem.id
		WHERE t.type IN (0, 6)
			AND t.system = FALSE
			AND s.id = t.schema_id
			AND ts.table_type_id = t.type
			AND s.name <> 'tmp';

	INSERT INTO dump_statements(s) SELECT * FROM dump_table_constraint_type();

	--TODO where are the parenthesis in the column definition.
	--TODO COLUMN DEFINITIONS
	--TODO COMMENTS ON TABLE
	--TODO functions
	--TODO REMOTE TABLE
	--TODO PARTITION TABLE
	--TODO CREATE INDEX + COMMENT
	--TODO User Defined Types?

    INSERT INTO dump_statements(s) VALUES ('COMMIT;');

END;

CALL dump_database(TRUE);
SELECT * FROM dump_statements;

ROLLBACK;

