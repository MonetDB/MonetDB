-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

CREATE VIEW sys.dump_create_roles AS
  SELECT
    'CREATE ROLE ' || sys.dq(name) || ';' stmt,
    name user_name
    FROM sys.auths
   WHERE name NOT IN (SELECT name FROM sys.db_user_info)
     AND grantor <> 0;

CREATE VIEW sys.dump_create_users AS
  SELECT
    'CREATE USER ' || sys.dq(ui.name) || ' WITH ENCRYPTED PASSWORD ' ||
      sys.sq(sys.password_hash(ui.name)) ||
      ' NAME ' || sys.sq(ui.fullname) || ' SCHEMA sys' || ifthenelse(ui.schema_path = '"sys"', '', ' SCHEMA PATH ' || sys.sq(ui.schema_path)) || ';' stmt,
    ui.name user_name
    FROM sys.db_user_info ui, sys.schemas s
   WHERE ui.default_schema = s.id
     AND ui.name <> 'monetdb'
     AND ui.name <> '.snapshot';

CREATE VIEW sys.dump_create_schemas AS
  SELECT
    'CREATE SCHEMA ' || sys.dq(s.name) || ifthenelse(a.name <> 'sysadmin', ' AUTHORIZATION ' || sys.dq(a.name), ' ') || ';' stmt,
    s.name schema_name
    FROM sys.schemas s, sys.auths a
   WHERE s.authorization = a.id AND s.system = FALSE;

CREATE VIEW sys.dump_add_schemas_to_users AS
  SELECT
    'ALTER USER ' || sys.dq(ui.name) || ' SET SCHEMA ' || sys.dq(s.name) || ';' stmt,
    s.name schema_name,
    ui.name user_name
    FROM sys.db_user_info ui, sys.schemas s
   WHERE ui.default_schema = s.id
     AND ui.name <> 'monetdb'
     AND ui.name <> '.snapshot'
     AND s.name <> 'sys';

CREATE VIEW sys.dump_grant_user_privileges AS
  SELECT
    'GRANT ' || sys.dq(a2.name) || ' ' || ifthenelse(a1.name = 'public', 'PUBLIC', sys.dq(a1.name)) || ';' stmt,
    a2.name grantee,
    a1.name grantor
    FROM sys.auths a1, sys.auths a2, sys.user_role ur
   WHERE a1.id = ur.login_id AND a2.id = ur.role_id;

CREATE VIEW sys.dump_table_constraint_type AS
  SELECT
    'ALTER TABLE ' || sys.FQN(sch, tbl) || ' ADD CONSTRAINT ' || sys.DQ(con) || ' '||
      tpe || ' (' || GROUP_CONCAT(sys.DQ(col), ', ') || ');' stmt,
    sch schema_name,
    tbl table_name,
    con constraint_name
    FROM sys.describe_constraints GROUP BY sch, tbl, con, tpe;

CREATE VIEW sys.dump_table_grants AS
  WITH table_grants (sname, tname, grantee, grants, grantor, grantable)
  AS (SELECT s.name, t.name, a.name, sum(p.privileges), g.name, p.grantable
	FROM sys.schemas s, sys.tables t, sys.auths a, sys.privileges p, sys.auths g
       WHERE p.obj_id = t.id AND p.auth_id = a.id AND t.schema_id = s.id AND t.system = FALSE AND p.grantor = g.id
       GROUP BY s.name, t.name, a.name, g.name, p.grantable
       ORDER BY s.name, t.name, a.name, g.name, p.grantable)
  SELECT
    'GRANT ' || pc.privilege_code_name || ' ON TABLE ' || sys.FQN(sname, tname)
      || ' TO ' || ifthenelse(grantee = 'public', 'PUBLIC', sys.dq(grantee))
      || CASE WHEN grantable = 1 THEN ' WITH GRANT OPTION' ELSE '' END || ';' stmt,
    sname schema_name,
    tname table_name,
    grantee
    FROM table_grants LEFT OUTER JOIN sys.privilege_codes pc ON grants = pc.privilege_code_id;

CREATE VIEW sys.dump_column_grants AS
  SELECT
    'GRANT ' || pc.privilege_code_name || '(' || sys.dq(c.name) || ') ON ' || sys.FQN(s.name, t.name)
      || ' TO ' || ifthenelse(a.name = 'public', 'PUBLIC', sys.dq(a.name))
      || CASE WHEN p.grantable = 1 THEN ' WITH GRANT OPTION' ELSE '' END || ';' stmt,
    s.name schema_name,
    t.name table_name,
    c.name column_name,
    a.name grantee
    FROM sys.schemas s,
	 sys.tables t,
	 sys.columns c,
	 sys.auths a,
	 sys.privileges p,
	 sys.auths g,
	 sys.privilege_codes pc
   WHERE p.obj_id = c.id
     AND c.table_id = t.id
     AND p.auth_id = a.id
     AND t.schema_id = s.id
     AND NOT t.system
     AND p.grantor = g.id
     AND p.privileges = pc.privilege_code_id
   ORDER BY s.name, t.name, c.name, a.name, g.name, p.grantable;

CREATE VIEW sys.dump_function_grants AS
  WITH func_args_all(func_id, number, max_number, func_arg) AS
  (SELECT a.func_id,
	  a.number,
	  max(a.number) OVER (PARTITION BY a.func_id ORDER BY a.number DESC),
	  group_concat(sys.describe_type(a.type, a.type_digits, a.type_scale), ', ') OVER (PARTITION BY a.func_id ORDER BY a.number)
     FROM sys.args a
    WHERE a.inout = 1),
  func_args(func_id, func_arg) AS
  (SELECT func_id, func_arg FROM func_args_all WHERE number = max_number)
  SELECT
    'GRANT ' || pc.privilege_code_name || ' ON ' || ft.function_type_keyword || ' '
      || sys.FQN(s.name, f.name) || '(' || coalesce(fa.func_arg, '') || ') TO '
      || ifthenelse(a.name = 'public', 'PUBLIC', sys.dq(a.name))
      || CASE WHEN p.grantable = 1 THEN ' WITH GRANT OPTION' ELSE '' END || ';' stmt,
    s.name schema_name,
    f.name function_name,
    a.name grantee
    FROM sys.schemas s,
	 sys.functions f LEFT OUTER JOIN func_args fa ON f.id = fa.func_id,
	 sys.auths a,
	 sys.privileges p,
	 sys.auths g,
	 sys.function_types ft,
	 sys.privilege_codes pc
   WHERE s.id = f.schema_id
     AND f.id = p.obj_id
     AND p.auth_id = a.id
     AND p.grantor = g.id
     AND p.privileges = pc.privilege_code_id
     AND f.type = ft.function_type_id
     AND NOT f.system
   ORDER BY s.name, f.name, a.name, g.name, p.grantable;

CREATE VIEW sys.dump_indices AS
  SELECT
    'CREATE ' || tpe || ' ' || sys.DQ(ind) || ' ON ' || sys.FQN(sch, tbl) || '(' || GROUP_CONCAT(col) || ');' stmt,
    sch schema_name,
    tbl table_name,
    ind index_name
    FROM sys.describe_indices GROUP BY ind, tpe, sch, tbl;

CREATE VIEW sys.dump_column_defaults AS
  SELECT 'ALTER TABLE ' || sys.FQN(sch, tbl) || ' ALTER COLUMN ' || sys.DQ(col) || ' SET DEFAULT ' || def || ';' stmt,
	 sch schema_name,
	 tbl table_name,
	 col column_name
    FROM sys.describe_column_defaults;

CREATE VIEW sys.dump_foreign_keys AS
  SELECT
    'ALTER TABLE ' || sys.FQN(fk_s, fk_t) || ' ADD CONSTRAINT ' || sys.DQ(fk) || ' ' ||
      'FOREIGN KEY(' || GROUP_CONCAT(sys.DQ(fk_c), ',') ||') ' ||
      'REFERENCES ' || sys.FQN(pk_s, pk_t) || '(' || GROUP_CONCAT(sys.DQ(pk_c), ',') || ') ' ||
      'ON DELETE ' || on_delete || ' ON UPDATE ' || on_update ||
      ';' stmt,
    fk_s foreign_schema_name,
    fk_t foreign_table_name,
    pk_s primary_schema_name,
    pk_t primary_table_name,
    fk key_name
    FROM sys.describe_foreign_keys GROUP BY fk_s, fk_t, pk_s, pk_t, fk, on_delete, on_update;

CREATE VIEW sys.dump_partition_tables AS
  SELECT
    'ALTER TABLE ' || sys.FQN(m_sch, m_tbl) || ' ADD TABLE ' || sys.FQN(p_sch, p_tbl) ||
      CASE
      WHEN tpe = 'VALUES' THEN ' AS PARTITION IN (' || pvalues || ')'
      WHEN tpe = 'RANGE' THEN ' AS PARTITION FROM ' || ifthenelse(minimum IS NOT NULL, sys.SQ(minimum), 'RANGE MINVALUE') || ' TO ' || ifthenelse(maximum IS NOT NULL, sys.SQ(maximum), 'RANGE MAXVALUE')
      WHEN tpe = 'FOR NULLS' THEN ' AS PARTITION FOR NULL VALUES'
      ELSE '' --'READ ONLY'
      END ||
      CASE WHEN tpe in ('VALUES', 'RANGE') AND with_nulls THEN ' WITH NULL VALUES' ELSE '' END ||
      ';' stmt,
    m_sch merge_schema_name,
    m_tbl merge_table_name,
    p_sch partition_schema_name,
    p_tbl partition_table_name
    FROM sys.describe_partition_tables;

CREATE VIEW sys.dump_sequences AS
  SELECT
    'CREATE SEQUENCE ' || sys.FQN(sch, seq) || ' AS BIGINT;' stmt,
    sch schema_name,
    seq seqname
    FROM sys.describe_sequences;

CREATE VIEW sys.dump_start_sequences AS
  SELECT 'ALTER SEQUENCE ' || sys.FQN(sch, seq) ||
	   CASE WHEN s = 0 THEN '' ELSE ' RESTART WITH ' || rs END ||
	   CASE WHEN inc = 1 THEN '' ELSE ' INCREMENT BY ' || inc END ||
	   CASE WHEN nomin THEN ' NO MINVALUE' WHEN rmi IS NULL THEN '' ELSE ' MINVALUE ' || rmi END ||
	   CASE WHEN nomax THEN ' NO MAXVALUE' WHEN rma IS NULL THEN '' ELSE ' MAXVALUE ' || rma END ||
	   CASE WHEN "cache" = 1 THEN '' ELSE ' CACHE ' || "cache" END ||
	   CASE WHEN "cycle" THEN '' ELSE ' NO' END || ' CYCLE;' stmt,
    sch schema_name,
    seq sequence_name
    FROM sys.describe_sequences;

CREATE VIEW sys.dump_functions AS
  SELECT f.o o, sys.schema_guard(f.sch, f.fun, f.def) stmt,
	 f.sch schema_name,
	 f.fun function_name
    FROM sys.describe_functions f;

CREATE VIEW sys.dump_tables AS
  SELECT
    t.o o,
    CASE
      WHEN t.typ <> 'VIEW' THEN
      'CREATE ' || t.typ || ' ' || sys.FQN(t.sch, t.tab) || t.col || t.opt || ';'
      ELSE
      t.opt
      END stmt,
    t.sch schema_name,
    t.tab table_name
    FROM sys.describe_tables t;

CREATE VIEW sys.dump_triggers AS
  SELECT sys.schema_guard(sch, tab, def) stmt,
	 sch schema_name,
	 tab table_name,
	 tri trigger_name
    FROM sys.describe_triggers;

CREATE VIEW sys.dump_comments AS
  SELECT 'COMMENT ON ' || c.tpe || ' ' || c.fqn || ' IS ' || sys.SQ(c.rem) || ';' stmt FROM sys.describe_comments c;

CREATE VIEW sys.dump_user_defined_types AS
  SELECT 'CREATE TYPE ' || sys.FQN(sch, sql_tpe) || ' EXTERNAL NAME ' || sys.DQ(ext_tpe) || ';' stmt,
	 sch schema_name,
	 sql_tpe type_name
    FROM sys.describe_user_defined_types;

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

CREATE PROCEDURE sys.dump_table_data(sch STRING, tbl STRING)
BEGIN
  DECLARE tid INT;
  SET tid = (SELECT MIN(t.id) FROM sys.tables t, sys.schemas s WHERE t.name = tbl AND t.schema_id = s.id AND s.name = sch);
  IF tid IS NOT NULL THEN
    DECLARE k INT;
    DECLARE m INT;
    SET k = (SELECT MIN(c.id) FROM sys.columns c WHERE c.table_id = tid);
    SET m = (SELECT MAX(c.id) FROM sys.columns c WHERE c.table_id = tid);
    IF k IS NOT NULL AND m IS NOT NULL THEN
      DECLARE cname STRING;
      DECLARE ctype STRING;
      DECLARE _cnt INT;
      SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);
      SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);
      SET _cnt = (SELECT count FROM sys.storage(sch, tbl, cname));
      IF _cnt > 0 THEN
        DECLARE COPY_INTO_STMT STRING;
        DECLARE SELECT_DATA_STMT STRING;
        SET COPY_INTO_STMT = 'COPY ' || _cnt || ' RECORDS INTO ' || sys.FQN(sch, tbl) || '(' || sys.DQ(cname);
        SET SELECT_DATA_STMT = 'SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), ' || sys.prepare_esc(cname, ctype);
        WHILE (k < m) DO
          SET k = (SELECT MIN(c.id) FROM sys.columns c WHERE c.table_id = tid AND c.id > k);
          SET cname = (SELECT c.name FROM sys.columns c WHERE c.id = k);
          SET ctype = (SELECT c.type FROM sys.columns c WHERE c.id = k);
          SET COPY_INTO_STMT = (COPY_INTO_STMT || ', ' || sys.DQ(cname));
          SET SELECT_DATA_STMT = (SELECT_DATA_STMT || '|| ''|'' || ' || sys.prepare_esc(cname, ctype));
        END WHILE;
        SET COPY_INTO_STMT = (COPY_INTO_STMT || ') FROM STDIN USING DELIMITERS ''|'',E''\\n'',''"'';');
        SET SELECT_DATA_STMT = (SELECT_DATA_STMT || ' FROM ' || sys.FQN(sch, tbl));
        INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, COPY_INTO_STMT);
        CALL sys.EVAL('INSERT INTO sys.dump_statements ' || SELECT_DATA_STMT || ';');
      END IF;
    END IF;
  END IF;
END;

CREATE PROCEDURE sys.dump_table_data()
BEGIN
  DECLARE i INT;
  SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system);
  IF i IS NOT NULL THEN
    DECLARE M INT;
    SET M = (SELECT MAX(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system);
    DECLARE sch STRING;
    DECLARE tbl STRING;
    WHILE i IS NOT NULL AND i <= M DO
      SET sch = (SELECT s.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);
      SET tbl = (SELECT t.name FROM sys.tables t, sys.schemas s WHERE s.id = t.schema_id AND t.id = i);
      CALL sys.dump_table_data(sch, tbl);
      SET i = (SELECT MIN(t.id) FROM sys.tables t, sys.table_types ts WHERE t.type = ts.table_type_id AND ts.table_type_name = 'TABLE' AND NOT t.system AND t.id > i);
    END WHILE;
  END IF;
END;

CREATE FUNCTION sys.dump_database(describe BOOLEAN) RETURNS TABLE(o int, stmt STRING)
BEGIN
  SET SCHEMA sys;
  TRUNCATE sys.dump_statements;
  INSERT INTO sys.dump_statements VALUES (1, 'START TRANSACTION;');
  INSERT INTO sys.dump_statements VALUES (2, 'SET SCHEMA "sys";');
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_roles;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_users;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_create_schemas;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_user_defined_types;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_add_schemas_to_users;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_grant_user_privileges;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_sequences;

  --functions and table-likes can be interdependent. They should be inserted in the order of their catalogue id.
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(ORDER BY stmts.o), stmts.s
				    FROM (
				      SELECT f.o, f.stmt FROM sys.dump_functions f
				       UNION ALL
				      SELECT t.o, t.stmt FROM sys.dump_tables t
				    ) AS stmts(o, s);

  -- dump table data before adding constraints and fixing sequences
  IF NOT DESCRIBE THEN
    CALL sys.dump_table_data();
  END IF;

  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_start_sequences;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_column_defaults;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_table_constraint_type;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_indices;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_foreign_keys;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_partition_tables;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_triggers;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_comments;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_table_grants;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_column_grants;
  INSERT INTO sys.dump_statements SELECT (SELECT COUNT(*) FROM sys.dump_statements) + RANK() OVER(), stmt FROM sys.dump_function_grants;

  --TODO Improve performance of dump_table_data.
  --TODO loaders ,procedures, window and filter sys.functions.
  --TODO look into order dependent group_concat

  INSERT INTO sys.dump_statements VALUES ((SELECT COUNT(*) FROM sys.dump_statements) + 1, 'COMMIT;');

  RETURN sys.dump_statements;
END;
