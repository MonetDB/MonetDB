START TRANSACTION;

CREATE FUNCTION SQ (s STRING) RETURNS STRING BEGIN RETURN ' ''' || s || ''' '; END;
CREATE FUNCTION DQ (s STRING) RETURNS STRING BEGIN RETURN '"' || s || '"'; END; --TODO: Figure out why this breaks with the space
CREATE FUNCTION I (s STRING) RETURNS STRING BEGIN RETURN '\t' || s || '\n'; END;
CREATE FUNCTION ENI (s STRING) RETURNS STRING BEGIN RETURN I(SQ(s)); END;

CREATE FUNCTION comment_on(ob STRING, id STRING, r STRING) RETURNS STRING BEGIN RETURN ifthenelse(r IS NOT NULL, '\nCOMMENT ON ' || ob ||  ' ' || id || ' IS ' || SQ(r) || ';', ''); END;

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

    RETURN 
        'START TRANSACTION;\n' ||
        create_roles || '\n' ||
        create_users || '\n' ||
        create_schemas || '\n' ||
        alter_users || '\n' ||
        grant_user_priviledges || '\n' ||
        create_sequences || '\n' ||
		--create_tables || '\n' ||
        'COMMIT;';

END;

SELECT dump_database(TRUE);

ROLLBACK;
