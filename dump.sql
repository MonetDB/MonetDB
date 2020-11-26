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
			GROUP_CONCAT(
				DQ(c.name) || ' ' ||
				dump_type(c.type, c.type_digits, c.type_scale) ||
				ifthenelse(c."null" = 'false', ' NOT NULL ', '') ||
				ifthenelse(c."default" IS NOT NULL, ' DEFAULT ' || c."default", '')
			, '')
			--c.number
		FROM sys._columns c 
		WHERE c.table_id = tid;
	END;

CREATE FUNCTION dump_database(describe BOOLEAN)
RETURNS STRING
BEGIN

    set schema sys;

    DECLARE create_roles STRING;
    SET create_roles = (
        SELECT GROUP_CONCAT('CREATE ROLE ' || name || ';')  FROM auths
        WHERE name NOT IN (SELECT name FROM db_user_info) 
        AND grantor <> 0
    );

    IF create_roles IS NULL THEN
        SET create_roles = '';
    END IF;

    declare create_users STRING;
    SET create_users = (SELECT
        GROUP_CONCAT(
        'CREATE USER ' ||  ui.name ||  ' WITH ENCRYPTED PASSWORD\n' ||
            ENI(password_hash(ui.name)) ||
        'NAME ' || ui.fullname ||  ' SCHEMA sys;', '\n')
        FROM db_user_info ui, schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot');

    IF create_users IS NULL THEN
        SET create_users = '';
    END IF;

    declare create_schemas STRING;
    SET create_schemas = (
        SELECT
            GROUP_CONCAT('CREATE SCHEMA ' ||  s.name || ifthenelse(a.name <> 'sysadmin', ' AUTHORIZATION ' || a.name, ' ') || ';' || 
            comment_on('SCHEMA', s.name, rem.remark), '\n')
        FROM schemas s LEFT OUTER JOIN comments rem ON s.id = rem.id,auths a
        WHERE s.authorization = a.id AND s.system = FALSE);

    IF create_schemas IS NULL THEN
        SET create_schemas = '';
    END IF;

    declare alter_users STRING;
    SET alter_users = (
        SELECT
            GROUP_CONCAT('ALTER USER ' || ui.name || ' SET SCHEMA ' || s.name || ';', '\n')
        FROM db_user_info ui, schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot'
            AND s.name <> 'sys');

    IF alter_users IS NULL THEN
        SET alter_users = '';
    END IF;

    declare grant_user_priviledges STRING;
    SET grant_user_priviledges = (
        SELECT
            GROUP_CONCAT('GRANT ' || DQ(a2.name) || ' ' || ifthenelse(a1.name = 'public', 'PUBLIC', DQ(a1.name)) || ';', '\n')
		FROM sys.auths a1, sys.auths a2, sys.user_role ur
		WHERE a1.id = ur.login_id AND a2.id = ur.role_id);

    IF grant_user_priviledges IS NULL THEN
        SET grant_user_priviledges = '';
    END IF;


	declare create_sequences STRING;
	SET create_sequences  = (
		SELECT GROUP_CONCAT('CREATE SEQUENCE ' || DQ(sch.name) || '.' || DQ(seq.name) || ' AS INTEGER;' || 
		comment_on('SEQUENCE', DQ(sch.name) || '.' || DQ(seq.name), rem.remark), '\n')
		FROM sys.schemas sch,
			sys.sequences seq LEFT OUTER JOIN sys.comments rem ON seq.id = rem.id
		WHERE sch.id = seq.schema_id);

    IF create_sequences IS NULL THEN
        SET create_sequences = '';
    END IF;

	declare create_tables STRING;
    set create_tables = (
		SELECT GROUP_CONCAT('CREATE ' || ts.table_type_name || ' ' || DQ(s.name) || '.' || DQ(t.name) || dump_column_definition(t.id) || ';', '\n')
		FROM sys.schemas s, table_types ts, sys._tables t LEFT OUTER JOIN sys.comments rem ON t.id = rem.id
		WHERE t.type IN (0, 6)
			AND t.system = FALSE
			AND s.id = t.schema_id
			AND ts.table_type_id = t.type
			AND s.name <> 'tmp');

    IF create_tables IS NULL THEN
        SET create_tables = '';
    END IF;

	--TODO where are the parenthesis in the column definition.
	--TODO COLUMN DEFINITIONS
	--TODO COMMENTS ON TABLE
	--TODO functions
	--TODO REMOTE TABLE
	--TODO PARTITION TABLE
	--TODO CREATE INDEX + COMMENT
	--TODO User Defined Types?

    RETURN 
        'START TRANSACTION;\n' ||
        create_roles || '\n' ||
        create_users || '\n' ||
        create_schemas || '\n' ||
        alter_users || '\n' ||
        grant_user_priviledges || '\n' ||
        create_sequences || '\n' ||
		create_tables || '\n' ||
        'COMMIT;';

END;

SELECT dump_database(TRUE);

ROLLBACK;

