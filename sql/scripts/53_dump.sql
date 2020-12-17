CREATE FUNCTION dump_table_constraint_type() RETURNS TABLE(stm STRING) BEGIN
	RETURN
		SELECT
			'ALTER TABLE ' || DQ(s) || '.' || DQ("table") ||
			' ADD CONSTRAINT ' || DQ(con) || ' '||
			type || ' (' || GROUP_CONCAT(DQ(col), ', ') || ');'
		FROM describe_constraints() GROUP BY s, "table", con, type;
END;

CREATE FUNCTION dump_indices() RETURNS TABLE(stm STRING) BEGIN
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
