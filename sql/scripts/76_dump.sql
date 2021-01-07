CREATE VIEW dump_table_constraint_type AS
	SELECT
		'ALTER TABLE ' || DQ(s) || '.' || DQ("table") ||
		' ADD CONSTRAINT ' || DQ(con) || ' '||
		type || ' (' || GROUP_CONCAT(DQ(col), ', ') || ');' stmt
	FROM describe_constraints GROUP BY s, "table", con, type;

CREATE VIEW dump_indices AS
	SELECT
		'CREATE ' || it || ' ' ||
		DQ(i) || ' ON ' || DQ(s) || '.' || DQ(t) ||
		'(' || GROUP_CONCAT(c) || ');' stmt
	FROM describe_indices GROUP BY i, it, s, t;

CREATE VIEW dump_column_defaults AS
	SELECT 'ALTER TABLE ' || FQN(sch, tbl) || ' ALTER COLUMN ' || DQ(col) || ' SET DEFAULT ' || def || ';' stmt
	FROM describe_column_defaults;

CREATE FUNCTION dump_foreign_keys AS
	SELECT
		'ALTER TABLE ' || DQ(fk_s) || '.'|| DQ(fk_t) || ' ADD CONSTRAINT ' || DQ(fk) || ' ' ||
		'FOREIGN KEY(' || GROUP_CONCAT(DQ(fk_c), ',') ||') ' ||
		'REFERENCES ' || DQ(pk_s) || '.' || DQ(pk_t) || '(' || GROUP_CONCAT(DQ(pk_c), ',') || ') ' ||
		'ON DELETE ' || on_delete || ' ON UPDATE ' || on_update ||
		';' stmt
	FROM describe_foreign_keys GROUP BY fk_s, fk_t, pk_s, pk_t, fk, on_delete, on_update;

CREATE FUNCTION dump_partition_tables AS
	SELECT
		ALTER_TABLE(m_sname, m_tname) || ' ADD TABLE ' || FQN(p_sname, p_tname) ||
		CASE 
			WHEN p_type = 'VALUES' THEN ' AS PARTITION IN (' || pvalues || ')'
			WHEN p_type = 'RANGE' THEN ' AS PARTITION FROM ' || ifthenelse(minimum IS NOT NULL, SQ(minimum), 'RANGE MINVALUE') || ' TO ' || ifthenelse(maximum IS NOT NULL, SQ(maximum), 'RANGE MAXVALUE')
			WHEN p_type = 'FOR NULLS' THEN ' AS PARTITION FOR NULL VALUES'
			ELSE '' --'READ ONLY'
		END ||
		CASE WHEN p_type in ('VALUES', 'RANGE') AND with_nulls THEN ' WITH NULL VALUES' ELSE '' END ||
		';' stmt
	FROM describe_partition_tables;

CREATE FUNCTION dump_sequences() RETURNS TABLE(stmt STRING) BEGIN
RETURN
	SELECT
		'CREATE SEQUENCE ' || FQN(sch, seq) || ' AS BIGINT ' ||
		CASE WHEN "s" <> 0 THEN 'START WITH ' || "rs" ELSE '' END ||
		CASE WHEN "inc" <> 1 THEN ' INCREMENT BY ' || "inc" ELSE '' END ||
		CASE WHEN "mi" <> 0 THEN ' MINVALUE ' || "mi" ELSE '' END ||
		CASE WHEN "ma" <> 0 THEN ' MAXVALUE ' || "ma" ELSE '' END ||
		CASE WHEN "cache" <> 1 THEN ' CACHE ' || "cache" ELSE '' END ||
		CASE WHEN "cycle" THEN ' CYCLE' ELSE '' END || ';'
	FROM describe_sequences;
END;

CREATE FUNCTION dump_start_sequences() RETURNS TABLE(stmt STRING) BEGIN
RETURN
	SELECT
		'UPDATE sys.sequences seq SET start = ' || s  ||
		' WHERE name = ' || SQ(seq) ||
		' AND schema_id = (SELECT s.id FROM sys.schemas s WHERE s.name = ' || SQ(sch) || ');'
	FROM describe_sequences;
END;

CREATE FUNCTION dump_functions() RETURNS TABLE (o INT, stmt STRING) BEGIN
	RETURN SELECT f.o, schema_guard(f.sch, f.fun, f.def)  FROM describe_functions f;
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
	FROM describe_tables t;
END;

CREATE FUNCTION dump_triggers() RETURNS TABLE (stmt STRING) BEGIN
	RETURN
		SELECT schema_guard(sch, tab, def) FROM describe_triggers;
END;

CREATE FUNCTION dump_comments() RETURNS TABLE(stmt STRING) BEGIN
RETURN
	SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || SQ(c.rem) || ';' FROM describe_comments c;
END;

--CREATE FUNCTION sys.describe_user_defined_types RETURNS TABLE(sch STRING, sql_tpe STRING, ext_tpe STRING)  BEGIN
CREATE FUNCTION sys.dump_user_defined_types() RETURNS TABLE(stmt STRING)  BEGIN
	RETURN
		SELECT 'CREATE TYPE ' || FQN(sch, sql_tpe) || ' EXTERNAL NAME ' || DQ(ext_tpe) || ';' FROM sys.describe_user_defined_types;
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
					'(SELECT fqn.id FROM fully_qualified_functions fqn WHERE' ||
						' fqn.nme = ' || SQ(dp.o_nme) || ' AND fqn.tpe = ' || SQ(dp.o_tpe) || '),'
			END ||
			'(SELECT id FROM auths a WHERE a.name = ' || SQ(dp.a_nme) || '),' ||
			'(SELECT pc.privilege_code_id FROM privilege_codes pc WHERE pc.privilege_code_name = ' || SQ(p_nme) || '),'
			'(SELECT id FROM auths g WHERE g.name = ' || SQ(dp.g_nme) || '),' ||
			dp.grantable ||
		');'
	FROM describe_privileges dp;
END;

CREATE PROCEDURE EVAL(stmt STRING) EXTERNAL NAME sql.eval;

CREATE FUNCTION esc(s STRING) RETURNS STRING BEGIN RETURN '"' || sys.replace(sys.replace(sys.replace(s,'\\', '\\\\'), '\n', '\\n'), '"', '\\"') || '"'; END;

CREATE FUNCTION prepare_esc(s STRING, t STRING) RETURNS STRING
BEGIN
    RETURN
        CASE
            WHEN (t = 'varchar' OR t ='char' OR t = 'clob' OR t = 'json' OR t = 'geometry' OR t = 'url') THEN
                'CASE WHEN ' || DQ(s) || ' IS NULL THEN ''null'' ELSE ' || 'esc(' || DQ(s) || ')' || ' END'
            ELSE
                'CASE WHEN ' || DQ(s) || ' IS NULL THEN ''null'' ELSE CAST(' || DQ(s) || ' AS STRING) END'
        END;
END;

--The dump statement should normally have an auto-incremented column representing the creation order.
--But in cases of db objects that can be interdependent, i.e. sys.functions and table-likes, we need access to the underlying sequence of the AUTO_INCREMENT property.
--Because we need to explicitly overwrite the creation order column "o" in those cases. After inserting the dump statements for sys.functions and table-likes,
--we can restart the auto-increment sequence with a sensible value for following dump statements.

CREATE TABLE sys.dump_statements(o INT, s STRING, PRIMARY KEY (o));

CREATE FUNCTION current_size_dump_statements() RETURNS INT BEGIN RETURN SELECT COUNT(*) FROM dump_statements; END;

CREATE PROCEDURE _dump_table_data(sch STRING, tbl STRING) BEGIN

    DECLARE k INT;
    SET k = (SELECT MIN(c.id) FROM columns c, tables t WHERE c.table_id = t.id AND t.name = tbl);
	IF k IS NOT NULL THEN

		DECLARE cname STRING;
		DECLARE ctype STRING;
		SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);
		SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);

		DECLARE COPY_INTO_STMT STRING;
		DECLARE _cnt INT;
		SET _cnt = (SELECT MIN(s.count) FROM sys.storage() s WHERE s.schema = sch AND s.table = tbl);

		IF _cnt > 0 THEN
			SET COPY_INTO_STMT = 'COPY ' || _cnt ||  ' RECORDS INTO ' || FQN(sch, tbl) || '(' || DQ(cname);

			DECLARE SELECT_DATA_STMT STRING;
			SET SELECT_DATA_STMT = 'SELECT current_size_dump_statements() + RANK() OVER(), ' || prepare_esc(cname, ctype);

			DECLARE M INT;
			SET M = (SELECT MAX(c.id) FROM columns c, tables t WHERE c.table_id = t.id AND t.name = tbl);

			WHILE (k < M) DO
				SET k = (SELECT MIN(c.id) FROM columns c, tables t WHERE c.table_id = t.id AND t.name = tbl AND c.id > k);
				SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);
				SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);
				SET COPY_INTO_STMT = (COPY_INTO_STMT || ', ' || DQ(cname));
				SET SELECT_DATA_STMT = SELECT_DATA_STMT || '|| ''|'' || ' || prepare_esc(cname, ctype);
			END WHILE;

			SET COPY_INTO_STMT = (COPY_INTO_STMT || ') FROM STDIN USING DELIMITERS ''|'',''\\n'',''"'';');
			SET SELECT_DATA_STMT =  SELECT_DATA_STMT || ' FROM ' || FQN(sch, tbl);

			insert into dump_statements VALUES (current_size_dump_statements() + 1, COPY_INTO_STMT);

			CALL sys.EVAL('INSERT INTO dump_statements ' || SELECT_DATA_STMT || ';');
		END IF;
	END IF;
END;

CREATE PROCEDURE dump_table_data() BEGIN

	DECLARE i INT;
    SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system);

	IF i IS NOT NULL THEN
		DECLARE M INT;
		SET M = (SELECT MAX(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system);

		DECLARE sch STRING;
		DECLARE tbl STRING;

		WHILE i < M DO
			set sch = (SELECT s.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);
			set tbl = (SELECT t.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);
			CALL _dump_table_data(sch, tbl);
			SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system AND t.id > i);
		END WHILE;

		set sch = (SELECT s.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);
		set tbl = (SELECT t.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);
		CALL _dump_table_data(sch, tbl);
	END IF;
END;

CREATE FUNCTION sys.dump_database(describe BOOLEAN) RETURNS TABLE(o int, stmt STRING)
BEGIN

	SET SCHEMA sys;
	TRUNCATE dump_statements;

	INSERT INTO dump_statements VALUES (1, 'START TRANSACTION;');
	INSERT INTO dump_statements VALUES (current_size_dump_statements() + 1, 'SET SCHEMA "sys";');

	INSERT INTO dump_statements --dump_create_roles
		SELECT current_size_dump_statements() + RANK() OVER(), 'CREATE ROLE ' || sys.dq(name) || ';' FROM sys.auths
        WHERE name NOT IN (SELECT name FROM sys.db_user_info)
        AND grantor <> 0;

	INSERT INTO dump_statements --dump_create_users
		SELECT current_size_dump_statements() + RANK() OVER(),
        'CREATE USER ' ||  sys.dq(ui.name) ||  ' WITH ENCRYPTED PASSWORD ' ||
            sys.sq(sys.password_hash(ui.name)) ||
        ' NAME ' || sys.sq(ui.fullname) ||  ' SCHEMA sys;'
        FROM sys.db_user_info ui, sys.schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot';

	INSERT INTO dump_statements --dump_create_schemas
        SELECT current_size_dump_statements() + RANK() OVER(),
            'CREATE SCHEMA ' ||  sys.dq(s.name) || ifthenelse(a.name <> 'sysadmin', ' AUTHORIZATION ' || a.name, ' ') || ';'
        FROM sys.schemas s, sys.auths a
        WHERE s.authorization = a.id AND s.system = FALSE;

	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_user_defined_types();

    INSERT INTO dump_statements --dump_add_schemas_to_users
	    SELECT current_size_dump_statements() + RANK() OVER(),
            'ALTER USER ' || sys.dq(ui.name) || ' SET SCHEMA ' || sys.dq(s.name) || ';'
        FROM sys.db_user_info ui, sys.schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot'
            AND s.name <> 'sys';

    INSERT INTO dump_statements --dump_grant_user_priviledges
        SELECT current_size_dump_statements() + RANK() OVER(),
            'GRANT ' || sys.dq(a2.name) || ' ' || ifthenelse(a1.name = 'public', 'PUBLIC', sys.dq(a1.name)) || ';'
		FROM sys.auths a1, sys.auths a2, sys.user_role ur
		WHERE a1.id = ur.login_id AND a2.id = ur.role_id;

	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_sequences();
	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_start_sequences();

	--functions and table-likes can be interdependent. They should be inserted in the order of their catalogue id.
	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(ORDER BY stmts.o), stmts.s
	FROM (
			SELECT * FROM sys.dump_functions() f
			UNION
			SELECT * FROM sys.dump_tables() t
		) AS stmts(o, s);

	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_column_defaults;
	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_table_constraint_type;
	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_indices;
	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_foreign_keys;
	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_partition_tables;
	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_triggers();
	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_comments();

	--We are dumping ALL privileges so we need to erase existing privileges on the receiving side;
	INSERT INTO dump_statements VALUES (current_size_dump_statements() + 1, 'TRUNCATE sys.privileges;');
	INSERT INTO dump_statements SELECT current_size_dump_statements() + RANK() OVER(), stmt FROM sys.dump_privileges();

	IF NOT DESCRIBE THEN
		CALL dump_table_data();
	END IF;
	--TODO clean up code: factor in more dump functions
	--TODO loaders ,procedures, window and filter sys.functions.
	--TODO look into order dependent group_concat
	--TODO ADD upgrade code

	RETURN dump_statements;
END;
