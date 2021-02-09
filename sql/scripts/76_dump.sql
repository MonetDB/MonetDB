CREATE VIEW sys.dump_create_roles AS
	SELECT
		'CREATE ROLE ' || sys.dq(name) || ';' stmt FROM sys.auths
	WHERE name NOT IN (SELECT name FROM sys.db_user_info)
	AND grantor <> 0;

CREATE VIEW sys.dump_create_users AS
	SELECT
		'CREATE USER ' ||  sys.dq(ui.name) ||  ' WITH ENCRYPTED PASSWORD ' ||
		sys.sq(sys.password_hash(ui.name)) ||
	' NAME ' || sys.sq(ui.fullname) ||  ' SCHEMA sys;' stmt
	FROM sys.db_user_info ui, sys.schemas s
	WHERE ui.default_schema = s.id
		AND ui.name <> 'monetdb'
		AND ui.name <> '.snapshot';

CREATE VIEW sys.dump_create_schemas AS
	SELECT
		'CREATE SCHEMA ' ||  sys.dq(s.name) || ifthenelse(a.name <> 'sysadmin', ' AUTHORIZATION ' || a.name, ' ') || ';' stmt
	FROM sys.schemas s, sys.auths a
	WHERE s.authorization = a.id AND s.system = FALSE;

CREATE VIEW sys.dump_add_schemas_to_users AS
	SELECT
		'ALTER USER ' || sys.dq(ui.name) || ' SET SCHEMA ' || sys.dq(s.name) || ';' stmt
	FROM sys.db_user_info ui, sys.schemas s
	WHERE ui.default_schema = s.id
		AND ui.name <> 'monetdb'
		AND ui.name <> '.snapshot'
		AND s.name <> 'sys';

CREATE VIEW sys.dump_grant_user_priviledges AS
	SELECT
		'GRANT ' || sys.dq(a2.name) || ' ' || ifthenelse(a1.name = 'public', 'PUBLIC', sys.dq(a1.name)) || ';' stmt
	FROM sys.auths a1, sys.auths a2, sys.user_role ur
	WHERE a1.id = ur.login_id AND a2.id = ur.role_id;

CREATE VIEW sys.dump_table_constraint_type AS
	SELECT
		'ALTER TABLE ' || DQ(sch) || '.' || DQ(tbl) ||
		' ADD CONSTRAINT ' || DQ(con) || ' '||
		tpe || ' (' || GROUP_CONCAT(DQ(col), ', ') || ');' stmt
	FROM describe_constraints GROUP BY sch, tbl, con, tpe;

CREATE VIEW sys.dump_indices AS
	SELECT
		'CREATE ' || tpe || ' ' ||
		DQ(ind) || ' ON ' || DQ(sch) || '.' || DQ(tbl) ||
		'(' || GROUP_CONCAT(col) || ');' stmt
	FROM describe_indices GROUP BY ind, tpe, sch, tbl;

CREATE VIEW sys.dump_column_defaults AS
	SELECT 'ALTER TABLE ' || FQN(sch, tbl) || ' ALTER COLUMN ' || DQ(col) || ' SET DEFAULT ' || def || ';' stmt
	FROM describe_column_defaults;

CREATE VIEW sys.dump_foreign_keys AS
	SELECT
		'ALTER TABLE ' || DQ(fk_s) || '.'|| DQ(fk_t) || ' ADD CONSTRAINT ' || DQ(fk) || ' ' ||
		'FOREIGN KEY(' || GROUP_CONCAT(DQ(fk_c), ',') ||') ' ||
		'REFERENCES ' || DQ(pk_s) || '.' || DQ(pk_t) || '(' || GROUP_CONCAT(DQ(pk_c), ',') || ') ' ||
		'ON DELETE ' || on_delete || ' ON UPDATE ' || on_update ||
		';' stmt
	FROM describe_foreign_keys GROUP BY fk_s, fk_t, pk_s, pk_t, fk, on_delete, on_update;

CREATE VIEW sys.dump_partition_tables AS
	SELECT
		ALTER_TABLE(m_sch, m_tbl) || ' ADD TABLE ' || FQN(p_sch, p_tbl) ||
		CASE 
			WHEN tpe = 'VALUES' THEN ' AS PARTITION IN (' || pvalues || ')'
			WHEN tpe = 'RANGE' THEN ' AS PARTITION FROM ' || ifthenelse(minimum IS NOT NULL, SQ(minimum), 'RANGE MINVALUE') || ' TO ' || ifthenelse(maximum IS NOT NULL, SQ(maximum), 'RANGE MAXVALUE')
			WHEN tpe = 'FOR NULLS' THEN ' AS PARTITION FOR NULL VALUES'
			ELSE '' --'READ ONLY'
		END ||
		CASE WHEN tpe in ('VALUES', 'RANGE') AND with_nulls THEN ' WITH NULL VALUES' ELSE '' END ||
		';' stmt
	FROM describe_partition_tables;

CREATE VIEW sys.dump_sequences AS
	SELECT
		'CREATE SEQUENCE ' || FQN(sch, seq) || ' AS BIGINT ' ||
		CASE WHEN "s" <> 0 THEN 'START WITH ' || "rs" ELSE '' END ||
		CASE WHEN "inc" <> 1 THEN ' INCREMENT BY ' || "inc" ELSE '' END ||
		CASE WHEN "mi" <> 0 THEN ' MINVALUE ' || "mi" ELSE '' END ||
		CASE WHEN "ma" <> 0 THEN ' MAXVALUE ' || "ma" ELSE '' END ||
		CASE WHEN "cache" <> 1 THEN ' CACHE ' || "cache" ELSE '' END ||
		CASE WHEN "cycle" THEN ' CYCLE' ELSE '' END || ';' stmt
	FROM describe_sequences;

CREATE VIEW sys.dump_start_sequences AS
	SELECT
		'UPDATE sys.sequences seq SET start = ' || s  ||
		' WHERE name = ' || SQ(seq) ||
		' AND schema_id = (SELECT s.id FROM sys.schemas s WHERE s.name = ' || SQ(sch) || ');' stmt
	FROM describe_sequences;

CREATE VIEW sys.dump_functions AS
	SELECT f.o o, schema_guard(f.sch, f.fun, f.def) stmt FROM describe_functions f;

CREATE VIEW sys.dump_tables AS
	SELECT
		t.o o,
		CASE
			WHEN t.typ <> 'VIEW' THEN
				'CREATE ' || t.typ || ' ' || FQN(t.sch, t.tab) || t.col || t.opt || ';'
			ELSE
				t.opt
		END stmt
	FROM describe_tables t;

CREATE VIEW sys.dump_triggers AS
	SELECT schema_guard(sch, tab, def) stmt FROM describe_triggers;

CREATE VIEW sys.dump_comments AS
	SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || SQ(c.rem) || ';' stmt FROM describe_comments c;

CREATE VIEW sys.dump_user_defined_types AS
		SELECT 'CREATE TYPE ' || FQN(sch, sql_tpe) || ' EXTERNAL NAME ' || DQ(ext_tpe) || ';' stmt FROM sys.describe_user_defined_types;

CREATE VIEW sys.dump_privileges AS
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
		');' stmt
	FROM describe_privileges dp;

CREATE PROCEDURE sys.EVAL(stmt STRING) EXTERNAL NAME sql.eval;

CREATE FUNCTION sys.esc(s STRING) RETURNS STRING BEGIN RETURN '"' || sys.replace(sys.replace(sys.replace(s,E'\\', E'\\\\'), E'\n', E'\\n'), '"', E'\\"') || '"'; END;

CREATE FUNCTION sys.prepare_esc(s STRING, t STRING) RETURNS STRING
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

CREATE TABLE sys.dump_statements(o INT, s STRING);

CREATE PROCEDURE sys._dump_table_data(sch STRING, tbl STRING) BEGIN

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
			SET SELECT_DATA_STMT = 'SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), ' || prepare_esc(cname, ctype);

			DECLARE M INT;
			SET M = (SELECT MAX(c.id) FROM columns c, tables t WHERE c.table_id = t.id AND t.name = tbl);

			WHILE (k < M) DO
				SET k = (SELECT MIN(c.id) FROM columns c, tables t WHERE c.table_id = t.id AND t.name = tbl AND c.id > k);
				SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);
				SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);
				SET COPY_INTO_STMT = (COPY_INTO_STMT || ', ' || DQ(cname));
				SET SELECT_DATA_STMT = SELECT_DATA_STMT || '|| ''|'' || ' || prepare_esc(cname, ctype);
			END WHILE;

			SET COPY_INTO_STMT = (COPY_INTO_STMT || ') FROM STDIN USING DELIMITERS ''|'',E''\\n'',''"'';');
			SET SELECT_DATA_STMT =  SELECT_DATA_STMT || ' FROM ' || FQN(sch, tbl);

			insert into dump_statements VALUES ((SELECT COUNT(*) FROM dump_statements) + 1, COPY_INTO_STMT);

			CALL sys.EVAL('INSERT INTO dump_statements ' || SELECT_DATA_STMT || ';');
		END IF;
	END IF;
END;

CREATE PROCEDURE sys.dump_table_data() BEGIN

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
	INSERT INTO dump_statements VALUES ((SELECT COUNT(*) FROM dump_statements) + 1, 'SET SCHEMA "sys";');
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_roles;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_users;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_schemas;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_user_defined_types;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_add_schemas_to_users;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_grant_user_priviledges;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_sequences;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_start_sequences;

	--functions and table-likes can be interdependent. They should be inserted in the order of their catalogue id.
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(ORDER BY stmts.o), stmts.s
	FROM (
			SELECT * FROM sys.dump_functions f
			UNION
			SELECT * FROM sys.dump_tables t
		) AS stmts(o, s);

	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_column_defaults;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_table_constraint_type;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_indices;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_foreign_keys;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_partition_tables;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_triggers;
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_comments;

	--We are dumping ALL privileges so we need to erase existing privileges on the receiving side;
	INSERT INTO dump_statements VALUES ((SELECT COUNT(*) FROM dump_statements) + 1, 'TRUNCATE sys.privileges;');
	INSERT INTO dump_statements SELECT (SELECT COUNT(*) FROM dump_statements) + RANK() OVER(), stmt FROM sys.dump_privileges;

	IF NOT DESCRIBE THEN
		CALL dump_table_data();
	END IF;
	--TODO Improve performance of dump_table_data.
	--TODO loaders ,procedures, window and filter sys.functions.
	--TODO look into order dependent group_concat

	INSERT INTO dump_statements VALUES ((SELECT COUNT(*) FROM dump_statements) + 1, 'COMMIT;');

	RETURN dump_statements;
END;
