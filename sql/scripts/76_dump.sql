CREATE FUNCTION dump_table_constraint_type() RETURNS TABLE(stmt STRING) BEGIN
	RETURN
		SELECT
			'ALTER TABLE ' || DQ(s) || '.' || DQ("table") ||
			' ADD CONSTRAINT ' || DQ(con) || ' '||
			type || ' (' || GROUP_CONCAT(DQ(col), ', ') || ');'
		FROM describe_constraints() GROUP BY s, "table", con, type;
END;

CREATE FUNCTION dump_indices() RETURNS TABLE(stmt STRING) BEGIN
	RETURN
		SELECT
			'CREATE ' || it || ' ' ||
			DQ(i) || ' ON ' || DQ(s) || '.' || DQ(t) ||
			'(' || GROUP_CONCAT(c) || ');'
		FROM describe_indices() GROUP BY i, it, s, t;
END;

CREATE FUNCTION dump_column_defaults() RETURNS TABLE(stmt STRING) BEGIN
	RETURN
		SELECT 'ALTER TABLE ' || FQN(sch, tbl) || ' ALTER COLUMN ' || DQ(col) || ' SET DEFAULT ' || def || ';'
		FROM describe_column_defaults();
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

CREATE FUNCTION dump_partition_tables() RETURNS TABLE(stmt STRING) BEGIN
RETURN
	SELECT
		ALTER_TABLE(m_sname, m_tname) || ' ADD TABLE ' || FQN(p_sname, p_tname) ||
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

CREATE FUNCTION dump_sequences() RETURNS TABLE(stmt STRING) BEGIN
RETURN
	SELECT
		'CREATE SEQUENCE ' || FQN(sch, seq) || ' AS BIGINT ' ||
		CASE WHEN "s" <> 0 THEN ' START WITH ' || "s" ELSE '' END ||
		CASE WHEN "inc" <> 1 THEN ' INCREMENT BY ' || "inc" ELSE '' END ||
		CASE WHEN "mi" <> 0 THEN ' MINVALUE ' || "mi" ELSE '' END ||
		CASE WHEN "ma" <> 0 THEN ' MAXVALUE ' || "ma" ELSE '' END ||
		CASE WHEN "cache" <> 1 THEN ' CACHE ' || "cache" ELSE '' END ||
		CASE WHEN "cycle" THEN ' CYCLE' ELSE '' END || ';'
	FROM describe_sequences();
END;

CREATE FUNCTION dump_functions() RETURNS TABLE (o INT, stmt STRING) BEGIN
	RETURN SELECT f.o, schema_guard(f.sch, f.fun, f.def)  FROM describe_functions() f;
END;

CREATE FUNCTION dump_tables() RETURNS TABLE (o INT, stmt STRING) BEGIN
RETURN
	SELECT
		t.o,
		CASE
			WHEN t.typ <> 'VIEW' THEN
				'CREATE ' || t.typ || ' ' || FQN(t.sch, t.tab) || t.col || t.opt || ';'
			ELSE
				t.opt
		END
	FROM describe_tables() t;
END;

CREATE FUNCTION dump_triggers() RETURNS TABLE (stmt STRING) BEGIN
	RETURN
		SELECT schema_guard(sch, tab, def) FROM describe_triggers();
END;

CREATE FUNCTION dump_comments() RETURNS TABLE(stmt STRING) BEGIN
RETURN
	SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || SQ(c.rem) || ';' FROM describe_comments() c;
END;

--CREATE FUNCTION sys.describe_user_defined_types() RETURNS TABLE(sch STRING, sql_tpe STRING, ext_tpe STRING)  BEGIN
CREATE FUNCTION sys.dump_user_defined_types() RETURNS TABLE(stmt STRING)  BEGIN
	RETURN
		SELECT 'CREATE TYPE ' || FQN(sch, sql_tpe) || ' EXTERNAL NAME ' || DQ(ext_tpe) || ';' FROM sys.describe_user_defined_types();
END;

CREATE FUNCTION dump_privileges() RETURNS TABLE (stmt STRING) BEGIN
RETURN
	SELECT
		'INSERT INTO sys.privileges VALUES (' ||
			CASE
				WHEN dp.o_tpe = 'GLOBAL' THEN
					'0,'
				WHEN dp.o_tpe = 'TABLE' THEN
					'(SELECT t.id FROM sys.schemas s, tables t WHERE s.id = t.schema_id' ||
						' AND s.name || ''.'' || t.name =' || SQ(dp.o_nme) || '),'
				WHEN dp.o_tpe = 'COLUMN' THEN
					'(SELECT c.id FROM sys.schemas s, tables t, columns c WHERE s.id = t.schema_id AND t.id = c.table_id' ||
						' AND s.name || ''.'' || t.name || ''.'' || c.name =' || SQ(dp.o_nme) || '),'
				ELSE -- FUNCTION-LIKE
					'(SELECT fqn.id FROM fully_qualified_functions() fqn WHERE' ||
						' fqn.nme = ' || SQ(dp.o_nme) || ' AND fqn.tpe = ' || SQ(dp.o_tpe) || '),'
			END ||
			'(SELECT id FROM auths a WHERE a.name = ' || SQ(dp.a_nme) || '),' ||
			'(SELECT pc.privilege_code_id FROM privilege_codes pc WHERE pc.privilege_code_name = ' || SQ(p_nme) || '),'
			'(SELECT id FROM auths g WHERE g.name = ' || SQ(dp.g_nme) || '),' ||
			dp.grantable ||
		');'
	FROM describe_privileges() dp;
END;

CREATE PROCEDURE EVAL(stmt STRING) EXTERNAL NAME sql.eval;

CREATE FUNCTION esc(s STRING) RETURNS STRING BEGIN RETURN '"' || sys.replace(sys.replace(sys.replace(s,'\\', '\\\\'), '\n', '\\n'), '"', '\\"') || '"'; END;

CREATE FUNCTION esc_null(s STRING) RETURNS STRING BEGIN RETURN CASE WHEN s IS NULL THEN 'null' ELSE s END; END;

CREATE FUNCTION prepare_esc(s STRING, t STRING) RETURNS STRING
BEGIN
    RETURN
        CASE
            WHEN (t = 'varchar' OR t ='char' OR t = 'clob' OR t = 'json' OR t = 'geometry' OR t = 'url') THEN
                'esc_null(esc(' || DQ(s) || '))'
            ELSE
                'esc_null(' || DQ(s) || ')'
        END;
END;

--The dump statement should normally have an auto-incremented column representing the creation order.
--But in cases of db objects that can be interdependent, i.e. sys.functions and table-likes, we need access to the underlying sequence of the AUTO_INCREMENT property.
--Because we need to explicitly overwrite the creation order column "o" in those cases. After inserting the dump statements for sys.functions and table-likes,
--we can restart the auto-increment sequence with a sensible value for following dump statements.

CREATE SEQUENCE sys._auto_increment;
CREATE TABLE sys.dump_statements(o INT DEFAULT NEXT VALUE FOR sys._auto_increment, s STRING, PRIMARY KEY (o));

--Because ALTER SEQUENCE statements are not allowed in procedures,
--we have to do a really nasty hack to restart the _auto_increment sequence.

CREATE FUNCTION sys.restart_sequence(sch STRING, seq STRING, val BIGINT) RETURNS BIGINT EXTERNAL NAME sql."restart";

CREATE FUNCTION sys.dump_database(describe BOOLEAN) RETURNS TABLE(o int, stmt STRING)
BEGIN

	SET SCHEMA sys;
	TRUNCATE dump_statements;
	DECLARE dummy_result BIGINT; --HACK: otherwise I cannot call restart_sequence.
	SET dummy_result = sys.restart_sequence('sys', '_auto_increment', 0);

	INSERT INTO dump_statements(s) VALUES ('START TRANSACTION;');
	INSERT INTO dump_statements(s) VALUES ('SET SCHEMA "sys";');

	INSERT INTO dump_statements(s) --dump_create_roles
		SELECT 'CREATE ROLE ' || sys.dq(name) || ';' FROM sys.auths
        WHERE name NOT IN (SELECT name FROM sys.db_user_info)
        AND grantor <> 0;

	INSERT INTO dump_statements(s) --dump_create_users
		SELECT
        'CREATE USER ' ||  sys.dq(ui.name) ||  ' WITH ENCRYPTED PASSWORD ' ||
            sys.sq(sys.password_hash(ui.name)) ||
        ' NAME ' || sys.sq(ui.fullname) ||  ' SCHEMA sys;'
        FROM sys.db_user_info ui, sys.schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot';

	INSERT INTO dump_statements(s) --dump_create_schemas
        SELECT
            'CREATE SCHEMA ' ||  sys.dq(s.name) || ifthenelse(a.name <> 'sysadmin', ' AUTHORIZATION ' || a.name, ' ') || ';'
        FROM sys.schemas s, sys.auths a
        WHERE s.authorization = a.id AND s.system = FALSE;

	INSERT INTO dump_statements(s) SELECT stmt FROM sys.dump_user_defined_types();

    INSERT INTO dump_statements(s) --dump_add_schemas_to_users
	    SELECT
            'ALTER USER ' || sys.dq(ui.name) || ' SET SCHEMA ' || sys.dq(s.name) || ';'
        FROM sys.db_user_info ui, sys.schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot'
            AND s.name <> 'sys';

    INSERT INTO dump_statements(s) --dump_grant_user_priviledges
        SELECT
            'GRANT ' || sys.dq(a2.name) || ' ' || ifthenelse(a1.name = 'public', 'PUBLIC', sys.dq(a1.name)) || ';'
		FROM sys.auths a1, sys.auths a2, sys.user_role ur
		WHERE a1.id = ur.login_id AND a2.id = ur.role_id;

	INSERT INTO dump_statements(s) SELECT stmt FROM sys.dump_sequences();

	--START OF COMPLICATED DEPENDENCY STUFF:
	--functions and table-likes can be interdependent. They should be inserted in the order of their catalogue id.
	DECLARE offs INT;
	SET offs = (SELECT max(o) FROM dump_statements) - (SELECT min(ids.id) FROM (select id from sys.tables union select id from sys.functions) ids(id));

	INSERT INTO dump_statements SELECT f.o + offs, f.stmt FROM sys.dump_functions() f;
	INSERT INTO dump_statements SELECT t.o + offs, t.stmt FROM sys.dump_tables() t;

	SET offs = (SELECT max(o) + 1 FROM dump_statements);
	SET dummy_result = sys.restart_sequence('sys', '_auto_increment', offs);
	--END OF COMPLICATED DEPENDENCY STUFF.

	INSERT INTO dump_statements(s) SELECT stmt FROM sys.dump_column_defaults();
	INSERT INTO dump_statements(s) SELECT stmt FROM sys.dump_table_constraint_type();
	INSERT INTO dump_statements(s) SELECT stmt FROM sys.dump_indices();
	INSERT INTO dump_statements(s) SELECT stmt FROM sys.dump_foreign_keys();
	INSERT INTO dump_statements(s) SELECT stmt FROM sys.dump_partition_tables();
	INSERT INTO dump_statements(s) SELECT stmt from sys.dump_triggers();
	INSERT INTO dump_statements(s) SELECT stmt FROM sys.dump_comments();

	--We are dumping ALL privileges so we need to erase existing privileges on the receiving side;
	INSERT INTO dump_statements(s) VALUES ('TRUNCATE sys.privileges;');
	INSERT INTO dump_statements(s) SELECT stmt FROM sys.dump_privileges();

	--TODO dumping table data
	--TODO ALTER SEQUENCE using RESTART WITH after importing table_data.
	--TODO loaders ,procedures, window and filter sys.functions.
	--TODO look into order dependent group_concat
	--TODO ADD upgrade code

	INSERT INTO dump_statements(s) VALUES ('COMMIT;');

	RETURN dump_statements;
END;
