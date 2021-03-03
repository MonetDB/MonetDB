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

CREATE VIEW sys.dump_grant_user_privileges AS
	SELECT
		'GRANT ' || sys.dq(a2.name) || ' ' || ifthenelse(a1.name = 'public', 'PUBLIC', sys.dq(a1.name)) || ';' stmt
	FROM sys.auths a1, sys.auths a2, sys.user_role ur
	WHERE a1.id = ur.login_id AND a2.id = ur.role_id;

CREATE VIEW sys.dump_table_constraint_type AS
	SELECT
		'ALTER TABLE ' || sys.DQ(sch) || '.' || sys.DQ(tbl) ||
		' ADD CONSTRAINT ' || sys.DQ(con) || ' '||
		tpe || ' (' || GROUP_CONCAT(sys.DQ(col), ', ') || ');' stmt
	FROM sys.describe_constraints GROUP BY sch, tbl, con, tpe;

CREATE VIEW sys.dump_indices AS
	SELECT
		'CREATE ' || tpe || ' ' ||
		sys.DQ(ind) || ' ON ' || sys.DQ(sch) || '.' || sys.DQ(tbl) ||
		'(' || GROUP_CONCAT(col) || ');' stmt
	FROM sys.describe_indices GROUP BY ind, tpe, sch, tbl;

CREATE VIEW sys.dump_column_defaults AS
	SELECT 'ALTER TABLE ' || sys.FQN(sch, tbl) || ' ALTER COLUMN ' || sys.DQ(col) || ' SET DEFAULT ' || def || ';' stmt
	FROM sys.describe_column_defaults;

CREATE VIEW sys.dump_foreign_keys AS
	SELECT
		'ALTER TABLE ' || sys.DQ(fk_s) || '.'|| sys.DQ(fk_t) || ' ADD CONSTRAINT ' || sys.DQ(fk) || ' ' ||
		'FOREIGN KEY(' || GROUP_CONCAT(sys.DQ(fk_c), ',') ||') ' ||
		'REFERENCES ' || sys.DQ(pk_s) || '.' || sys.DQ(pk_t) || '(' || GROUP_CONCAT(sys.DQ(pk_c), ',') || ') ' ||
		'ON DELETE ' || on_delete || ' ON UPDATE ' || on_update ||
		';' stmt
	FROM sys.describe_foreign_keys GROUP BY fk_s, fk_t, pk_s, pk_t, fk, on_delete, on_update;

CREATE VIEW sys.dump_partition_tables AS
	SELECT
		sys.ALTER_TABLE(m_sch, m_tbl) || ' ADD TABLE ' || sys.FQN(p_sch, p_tbl) ||
		CASE 
			WHEN tpe = 'VALUES' THEN ' AS PARTITION IN (' || pvalues || ')'
			WHEN tpe = 'RANGE' THEN ' AS PARTITION FROM ' || ifthenelse(minimum IS NOT NULL, sys.SQ(minimum), 'RANGE MINVALUE') || ' TO ' || ifthenelse(maximum IS NOT NULL, sys.SQ(maximum), 'RANGE MAXVALUE')
			WHEN tpe = 'FOR NULLS' THEN ' AS PARTITION FOR NULL VALUES'
			ELSE '' --'READ ONLY'
		END ||
		CASE WHEN tpe in ('VALUES', 'RANGE') AND with_nulls THEN ' WITH NULL VALUES' ELSE '' END ||
		';' stmt
	FROM sys.describe_partition_tables;

CREATE VIEW sys.dump_sequences AS
	SELECT
		'CREATE SEQUENCE ' || sys.FQN(sch, seq) || ' AS BIGINT ' ||
		CASE WHEN "s" <> 0 THEN 'START WITH ' || "rs" ELSE '' END ||
		CASE WHEN "inc" <> 1 THEN ' INCREMENT BY ' || "inc" ELSE '' END ||
		CASE WHEN "mi" <> 0 THEN ' MINVALUE ' || "mi" ELSE '' END ||
		CASE WHEN "ma" <> 0 THEN ' MAXVALUE ' || "ma" ELSE '' END ||
		CASE WHEN "cache" <> 1 THEN ' CACHE ' || "cache" ELSE '' END ||
		CASE WHEN "cycle" THEN ' CYCLE' ELSE '' END || ';' stmt
	FROM sys.describe_sequences;

CREATE VIEW sys.dump_start_sequences AS
	SELECT
		'UPDATE sys.sequences seq SET start = ' || s  ||
		' WHERE name = ' || sys.SQ(seq) ||
		' AND schema_id = (SELECT s.id FROM sys.schemas s WHERE s.name = ' || sys.SQ(sch) || ');' stmt
	FROM sys.describe_sequences;

CREATE VIEW sys.dump_functions AS
	SELECT f.o o, sys.schema_guard(f.sch, f.fun, f.def) stmt FROM sys.describe_functions f;

CREATE VIEW sys.dump_tables AS
	SELECT
		t.o o,
		CASE
			WHEN t.typ <> 'VIEW' THEN
				'CREATE ' || t.typ || ' ' || sys.FQN(t.sch, t.tab) || t.col || t.opt || ';'
			ELSE
				t.opt
		END stmt
	FROM sys.describe_tables t;

CREATE VIEW sys.dump_triggers AS
	SELECT sys.schema_guard(sch, tab, def) stmt FROM sys.describe_triggers;

CREATE VIEW sys.dump_comments AS
	SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || sys.SQ(c.rem) || ';' stmt FROM sys.describe_comments c;

CREATE VIEW sys.dump_user_defined_types AS
		SELECT 'CREATE TYPE ' || sys.FQN(sch, sql_tpe) || ' EXTERNAL NAME ' || sys.DQ(ext_tpe) || ';' stmt FROM sys.describe_user_defined_types;

CREATE VIEW sys.dump_privileges AS
	SELECT
		'INSERT INTO sys.privileges VALUES (' ||
			CASE
				WHEN dp.o_tpe = 'GLOBAL' THEN
					'0,'
				WHEN dp.o_tpe = 'TABLE' THEN
					'(SELECT t.id FROM sys.schemas s, sys.tables t WHERE s.id = t.schema_id' ||
						' AND s.name || ''.'' || t.name =' || sys.SQ(dp.o_nme) || '),'
				WHEN dp.o_tpe = 'COLUMN' THEN
					'(SELECT c.id FROM sys.schemas s, sys.tables t, sys.columns c WHERE s.id = t.schema_id AND t.id = c.table_id' ||
						' AND s.name || ''.'' || t.name || ''.'' || c.name =' || sys.SQ(dp.o_nme) || '),'
				ELSE -- FUNCTION-LIKE
					'(SELECT fqn.id FROM sys.fully_qualified_functions fqn WHERE' ||
						' fqn.nme = ' || sys.SQ(dp.o_nme) || ' AND fqn.tpe = ' || sys.SQ(dp.o_tpe) || '),'
			END ||
			'(SELECT id FROM sys.auths a WHERE a.name = ' || sys.SQ(dp.a_nme) || '),' ||
			'(SELECT pc.privilege_code_id FROM sys.privilege_codes pc WHERE pc.privilege_code_name = ' || sys.SQ(p_nme) || '),'
			'(SELECT id FROM sys.auths g WHERE g.name = ' || sys.SQ(dp.g_nme) || '),' ||
			dp.grantable ||
		');' stmt
	FROM sys.describe_privileges dp;

CREATE PROCEDURE sys.EVAL(stmt STRING) EXTERNAL NAME sql.eval;

CREATE FUNCTION sys.esc(s STRING) RETURNS STRING BEGIN RETURN '"' || sys.replace(sys.replace(sys.replace(s,E'\\', E'\\\\'), E'\n', E'\\n'), '"', E'\\"') || '"'; END;

CREATE FUNCTION sys.prepare_esc(s STRING, t STRING) RETURNS STRING
BEGIN
    RETURN
        CASE
            WHEN (t = 'varchar' OR t ='char' OR t = 'clob' OR t = 'json' OR t = 'geometry' OR t = 'url') THEN
                'CASE WHEN ' || sys.DQ(s) || ' IS NULL THEN ''null'' ELSE ' || 'sys.esc(' || sys.DQ(s) || ')' || ' END'
            ELSE
                'CASE WHEN ' || sys.DQ(s) || ' IS NULL THEN ''null'' ELSE CAST(' || sys.DQ(s) || ' AS STRING) END'
        END;
END;

--The dump statement should normally have an auto-incremented column representing the creation order.
--But in cases of db objects that can be interdependent, i.e. sys.functions and table-likes, we need access to the underlying sequence of the AUTO_INCREMENT property.
--Because we need to explicitly overwrite the creation order column "o" in those cases. After inserting the dump statements for sys.functions and table-likes,
--we can restart the auto-increment sequence with a sensible value for following dump statements.

CREATE TABLE sys.dump_statements(o INT, s STRING);

CREATE PROCEDURE sys._dump_table_data(sch STRING, tbl STRING) BEGIN

    DECLARE k INT;
    SET k = (SELECT MIN(c.id) FROM sys.columns c, sys.tables t WHERE c.table_id = t.id AND t.name = tbl);
	IF k IS NOT NULL THEN

		DECLARE cname STRING;
		DECLARE ctype STRING;
		SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);
		SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);

		DECLARE COPY_INTO_STMT STRING;
		DECLARE _cnt INT;
		SET _cnt = (SELECT MIN(s.count) FROM sys.storage() s WHERE s.schema = sch AND s.table = tbl);

		IF _cnt > 0 THEN
			SET COPY_INTO_STMT = 'COPY ' || _cnt ||  ' RECORDS INTO ' || sys.FQN(sch, tbl) || '(' || sys.DQ(cname);

			DECLARE SELECT_DATA_STMT STRING;
			SET SELECT_DATA_STMT = 'SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), ' || sys.prepare_esc(cname, ctype);

			DECLARE M INT;
			SET M = (SELECT MAX(c.id) FROM sys.columns c, sys.tables t WHERE c.table_id = t.id AND t.name = tbl);

			WHILE (k < M) DO
				SET k = (SELECT MIN(c.id) FROM sys.columns c, sys.tables t WHERE c.table_id = t.id AND t.name = tbl AND c.id > k);
				SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);
				SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);
				SET COPY_INTO_STMT = (COPY_INTO_STMT || ', ' || sys.DQ(cname));
				SET SELECT_DATA_STMT = SELECT_DATA_STMT || '|| ''|'' || ' || sys.prepare_esc(cname, ctype);
			END WHILE;

			SET COPY_INTO_STMT = (COPY_INTO_STMT || ') FROM STDIN USING DELIMITERS ''|'',E''\\n'',''"'';');
			SET SELECT_DATA_STMT =  SELECT_DATA_STMT || ' FROM ' || sys.FQN(sch, tbl);

			insert into sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, COPY_INTO_STMT);

			CALL sys.EVAL('INSERT INTO sys.dump_statements ' || SELECT_DATA_STMT || ';');
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
			CALL sys._dump_table_data(sch, tbl);
			SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system AND t.id > i);
		END WHILE;

		set sch = (SELECT s.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);
		set tbl = (SELECT t.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);
		CALL sys._dump_table_data(sch, tbl);
	END IF;
END;

CREATE FUNCTION sys.dump_database(describe BOOLEAN) RETURNS TABLE(o int, stmt STRING)
BEGIN

	SET SCHEMA sys;
	TRUNCATE sys.dump_statements;

	INSERT INTO sys.dump_statements VALUES (1, 'START TRANSACTION;');
	INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, 'SET SCHEMA "sys";');
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_roles;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_users;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_schemas;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_user_defined_types;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_add_schemas_to_users;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_grant_user_privileges;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_sequences;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_start_sequences;

	--functions and table-likes can be interdependent. They should be inserted in the order of their catalogue id.
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(ORDER BY stmts.o), stmts.s
	FROM (
			SELECT * FROM sys.dump_functions f
			UNION
			SELECT * FROM sys.dump_tables t
		) AS stmts(o, s);

	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_column_defaults;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_table_constraint_type;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_indices;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_foreign_keys;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_partition_tables;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_triggers;
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_comments;

	--We are dumping ALL privileges so we need to erase existing privileges on the receiving side;
	INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, 'TRUNCATE sys.privileges;');
	INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_privileges;

	IF NOT DESCRIBE THEN
		CALL sys.dump_table_data();
	END IF;
	--TODO Improve performance of dump_table_data.
	--TODO loaders ,procedures, window and filter sys.functions.
	--TODO look into order dependent group_concat

	INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, 'COMMIT;');

	RETURN sys.dump_statements;
END;
