-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

CREATE FUNCTION sys.describe_type(ctype string, digits integer, tscale integer)
  RETURNS string
BEGIN
  RETURN
    CASE ctype
      WHEN 'bigint' THEN 'BIGINT'
      WHEN 'blob' THEN
	CASE digits
	  WHEN 0 THEN 'BINARY LARGE OBJECT'
	  ELSE 'BINARY LARGE OBJECT(' || digits || ')'
	END
      WHEN 'boolean' THEN 'BOOLEAN'
      WHEN 'char' THEN
        CASE digits
          WHEN 1 THEN 'CHARACTER'
          ELSE 'CHARACTER(' || digits || ')'
        END
      WHEN 'clob' THEN
	CASE digits
	  WHEN 0 THEN 'CHARACTER LARGE OBJECT'
	  ELSE 'CHARACTER LARGE OBJECT(' || digits || ')'
	END
      WHEN 'date' THEN 'DATE'
      WHEN 'day_interval' THEN 'INTERVAL DAY'
      WHEN ctype = 'decimal' THEN
  	CASE
	  WHEN (digits = 1 AND tscale = 0) OR digits = 0 THEN 'DECIMAL'
	  WHEN tscale = 0 THEN 'DECIMAL(' || digits || ')'
	  WHEN digits = 39 THEN 'DECIMAL(' || 38 || ',' || tscale || ')'
	  WHEN digits = 19 AND (SELECT COUNT(*) = 0 FROM sys.types WHERE sqlname = 'hugeint' ) THEN 'DECIMAL(' || 18 || ',' || tscale || ')'
	  ELSE 'DECIMAL(' || digits || ',' || tscale || ')'
	END
      WHEN 'double' THEN
	CASE
	  WHEN digits = 53 and tscale = 0 THEN 'DOUBLE'
	  WHEN tscale = 0 THEN 'FLOAT(' || digits || ')'
	  ELSE 'FLOAT(' || digits || ',' || tscale || ')'
	END
      WHEN 'geometry' THEN
	CASE digits
	  WHEN 4 THEN 'GEOMETRY(POINT' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || tscale
            END || ')'
	  WHEN 8 THEN 'GEOMETRY(LINESTRING' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || tscale
            END || ')'
	  WHEN 16 THEN 'GEOMETRY(POLYGON' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || tscale
            END || ')'
	  WHEN 20 THEN 'GEOMETRY(MULTIPOINT' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || tscale
            END || ')'
	  WHEN 24 THEN 'GEOMETRY(MULTILINESTRING' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || tscale
            END || ')'
	  WHEN 28 THEN 'GEOMETRY(MULTIPOLYGON' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || tscale
            END || ')'
	  WHEN 32 THEN 'GEOMETRY(GEOMETRYCOLLECTION' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || tscale
            END || ')'
	  ELSE 'GEOMETRY'
        END
      WHEN 'hugeint' THEN 'HUGEINT'
      WHEN 'int' THEN 'INTEGER'
      WHEN 'month_interval' THEN
	CASE digits
	  WHEN 1 THEN 'INTERVAL YEAR'
	  WHEN 2 THEN 'INTERVAL YEAR TO MONTH'
	  WHEN 3 THEN 'INTERVAL MONTH'
	END
      WHEN 'real' THEN
	CASE
	  WHEN digits = 24 and tscale = 0 THEN 'REAL'
	  WHEN tscale = 0 THEN 'FLOAT(' || digits || ')'
	  ELSE 'FLOAT(' || digits || ',' || tscale || ')'
	END
      WHEN 'sec_interval' THEN
	CASE digits
	  WHEN 4 THEN 'INTERVAL DAY'
	  WHEN 5 THEN 'INTERVAL DAY TO HOUR'
	  WHEN 6 THEN 'INTERVAL DAY TO MINUTE'
	  WHEN 7 THEN 'INTERVAL DAY TO SECOND'
	  WHEN 8 THEN 'INTERVAL HOUR'
	  WHEN 9 THEN 'INTERVAL HOUR TO MINUTE'
	  WHEN 10 THEN 'INTERVAL HOUR TO SECOND'
	  WHEN 11 THEN 'INTERVAL MINUTE'
	  WHEN 12 THEN 'INTERVAL MINUTE TO SECOND'
	  WHEN 13 THEN 'INTERVAL SECOND'
	END
      WHEN 'smallint' THEN 'SMALLINT'
      WHEN 'time' THEN
	CASE digits
	  WHEN 1 THEN 'TIME'
	  ELSE 'TIME(' || (digits - 1) || ')'
	END
      WHEN 'timestamp' THEN
	CASE digits
	  WHEN 7 THEN 'TIMESTAMP'
	  ELSE 'TIMESTAMP(' || (digits - 1) || ')'
	END
      WHEN 'timestamptz' THEN
	CASE digits
	  WHEN 7 THEN 'TIMESTAMP'
	  ELSE 'TIMESTAMP(' || (digits - 1) || ')'
	END || ' WITH TIME ZONE'
      WHEN 'timetz' THEN
	CASE digits
	  WHEN 1 THEN 'TIME'
	  ELSE 'TIME(' || (digits - 1) || ')'
	END || ' WITH TIME ZONE'
      WHEN 'tinyint' THEN 'TINYINT'
      WHEN 'varchar' THEN 'CHARACTER VARYING(' || digits || ')'
      ELSE
        CASE
          WHEN lower(ctype) = ctype THEN upper(ctype)
          ELSE '"' || ctype || '"'
        END || CASE digits
	  WHEN 0 THEN ''
          ELSE '(' || digits || CASE tscale
	    WHEN 0 THEN ''
            ELSE ',' || tscale
          END || ')'
	END
    END;
END;

CREATE FUNCTION sys.SQ (s STRING) RETURNS STRING BEGIN RETURN ' ''' || sys.replace(s,'''','''''') || ''' '; END;
CREATE FUNCTION sys.DQ (s STRING) RETURNS STRING BEGIN RETURN '"' || sys.replace(s,'"','""') || '"'; END; --TODO: Figure out why this breaks with the space
CREATE FUNCTION sys.FQN(s STRING, t STRING) RETURNS STRING BEGIN RETURN sys.DQ(s) || '.' || sys.DQ(t); END;
CREATE FUNCTION sys.ALTER_TABLE(s STRING, t STRING) RETURNS STRING BEGIN RETURN 'ALTER TABLE ' || sys.FQN(s, t) || ' '; END;

--We need pcre to implement a header guard which means adding the schema of an object explicitely to its identifier.
CREATE FUNCTION sys.replace_first(ori STRING, pat STRING, rep STRING, flg STRING) RETURNS STRING EXTERNAL NAME "pcre"."replace_first";
CREATE FUNCTION sys.schema_guard(sch STRING, nme STRING, stmt STRING) RETURNS STRING BEGIN
RETURN
	SELECT sys.replace_first(stmt, '(\\s*"?' || sch ||  '"?\\s*\\.|)\\s*"?' || nme || '"?\\s*', ' ' || sys.FQN(sch, nme) || ' ', 'imsx');
END;

CREATE VIEW sys.describe_constraints AS
	SELECT
		s.name sch,
		t.name tbl,
		kc.name col,
		k.name con,
		CASE WHEN k.type = 0 THEN 'PRIMARY KEY' WHEN k.type = 1 THEN 'UNIQUE' END tpe
	FROM sys.schemas s, sys._tables t, sys.objects kc, sys.keys k
	WHERE kc.id = k.id
		AND k.table_id = t.id
		AND s.id = t.schema_id
		AND t.system = FALSE
		AND k.type in (0, 1)
		AND t.type IN (0, 6);

CREATE VIEW sys.describe_indices AS
	WITH it (id, idx) AS (VALUES (0, 'INDEX'), (4, 'IMPRINTS INDEX'), (5, 'ORDERED INDEX')) --UNIQUE INDEX wraps to INDEX.
	SELECT
		i.name ind,
		s.name sch,
		t.name tbl,
		c.name col,
		it.idx tpe
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

CREATE VIEW sys.describe_column_defaults AS
	SELECT
		s.name sch,
		t.name tbl,
		c.name col,
		c."default" def
	FROM sys.schemas s, sys.tables t, sys.columns c
	WHERE
		s.id = t.schema_id AND
		t.id = c.table_id AND
		s.name <> 'tmp' AND
		NOT t.system AND
		c."default" IS NOT NULL;

CREATE VIEW sys.describe_foreign_keys AS
		WITH action_type (id, act) AS (VALUES
			(0, 'NO ACTION'),
			(1, 'CASCADE'),
			(2, 'RESTRICT'),
			(3, 'SET NULL'),
			(4, 'SET DEFAULT'))
		SELECT
			fs.name fk_s,
			fkt.name fk_t,
			fkkc.name fk_c,
			fkkc.nr o,
			fkk.name fk,
			ps.name pk_s,
			pkt.name pk_t,
			pkkc.name pk_c,
			ou.act on_update,
			od.act on_delete
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
		AND (fkk."action" & 255)         = od.id
		AND ((fkk."action" >> 8) & 255)  = ou.id
		ORDER BY fkk.name, fkkc.nr;

--TODO: CRASHES when this function gets inlined into describe_tables
CREATE FUNCTION sys.get_merge_table_partition_expressions(tid INT) RETURNS STRING
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
					THEN 'ON ' || '(' || (SELECT sys.DQ(c.name) || ')' FROM sys.columns c WHERE c.id = tp.column_id)
					ELSE 'USING ' || '(' || tp.expression || ')' --generic expression
				END
			ELSE									--read only partition merge table.
				''
			END
		FROM (VALUES (tid)) t(id) LEFT JOIN sys.table_partitions tp ON t.id = tp.table_id;
END;

--TODO: gives mergejoin errors when inlined
CREATE FUNCTION sys.get_remote_table_expressions(s STRING, t STRING) RETURNS STRING BEGIN
	RETURN SELECT ' ON ' || sys.SQ(uri) || ' WITH USER ' || sys.SQ(username) || ' ENCRYPTED PASSWORD ' || sys.SQ("hash") FROM sys.remote_table_credentials(s ||'.' || t);
END;

CREATE VIEW sys.describe_tables AS
	SELECT
		t.id o,
		s.name sch,
		t.name tab,
		ts.table_type_name typ,
		(SELECT
			' (' ||
			GROUP_CONCAT(
				sys.DQ(c.name) || ' ' ||
				sys.describe_type(c.type, c.type_digits, c.type_scale) ||
				ifthenelse(c."null" = 'false', ' NOT NULL', '')
			, ', ') || ')'
		FROM sys._columns c
		WHERE c.table_id = t.id) col,
		CASE
			WHEN ts.table_type_name = 'REMOTE TABLE' THEN
				sys.get_remote_table_expressions(s.name, t.name)
			WHEN ts.table_type_name = 'MERGE TABLE' THEN
				sys.get_merge_table_partition_expressions(t.id)
			WHEN ts.table_type_name = 'VIEW' THEN
				sys.schema_guard(s.name, t.name, t.query)
			ELSE
				''
		END opt
	FROM sys.schemas s, sys.table_types ts, sys.tables t
	WHERE ts.table_type_name IN ('TABLE', 'VIEW', 'MERGE TABLE', 'REMOTE TABLE', 'REPLICA TABLE')
		AND t.system = FALSE
		AND s.id = t.schema_id
		AND ts.table_type_id = t.type
		AND s.name <> 'tmp';

CREATE VIEW sys.describe_triggers AS
		SELECT
			s.name sch,
			t.name tab,
			tr.name tri,
			tr.statement def
		FROM sys.schemas s, sys.tables t, sys.triggers tr
		WHERE s.id = t.schema_id AND t.id = tr.table_id AND NOT t.system;

CREATE VIEW sys.describe_comments AS
		SELECT
			o.id id,
			o.tpe tpe,
			o.nme fqn,
			c.remark rem
		FROM (
			SELECT id, 'SCHEMA', sys.DQ(name) FROM sys.schemas

			UNION ALL

			SELECT t.id, CASE WHEN ts.table_type_name = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END, sys.FQN(s.name, t.name)
			FROM sys.schemas s JOIN sys.tables t ON s.id = t.schema_id JOIN sys.table_types ts ON t.type = ts.table_type_id
			WHERE NOT s.name <> 'tmp'

			UNION ALL

			SELECT c.id, 'COLUMN', sys.FQN(s.name, t.name) || '.' || sys.DQ(c.name) FROM sys.columns c, sys.tables t, sys.schemas s WHERE c.table_id = t.id AND t.schema_id = s.id

			UNION ALL

			SELECT idx.id, 'INDEX', sys.FQN(s.name, idx.name) FROM sys.idxs idx, sys._tables t, sys.schemas s WHERE idx.table_id = t.id AND t.schema_id = s.id

			UNION ALL

			SELECT seq.id, 'SEQUENCE', sys.FQN(s.name, seq.name) FROM sys.sequences seq, sys.schemas s WHERE seq.schema_id = s.id

			UNION ALL

			SELECT f.id, ft.function_type_keyword, sys.FQN(s.name, f.name) FROM sys.functions f, sys.function_types ft, sys.schemas s WHERE f.type = ft.function_type_id AND f.schema_id = s.id

			) AS o(id, tpe, nme)
			JOIN sys.comments c ON c.id = o.id;

CREATE VIEW sys.fully_qualified_functions AS
	WITH fqn(id, tpe, sig, num) AS
	(
		SELECT
			f.id,
			ft.function_type_keyword,
			CASE WHEN a.type IS NULL THEN
				s.name || '.' || f.name || '()'
			ELSE
				s.name || '.' || f.name || '(' || group_concat(sys.describe_type(a.type, a.type_digits, a.type_scale), ',') OVER (PARTITION BY f.id ORDER BY a.number)  || ')'
			END,
			a.number
		FROM sys.schemas s, sys.function_types ft, sys.functions f LEFT JOIN sys.args a ON f.id = a.func_id
		WHERE s.id= f.schema_id AND f.type = ft.function_type_id
	)
	SELECT
		fqn1.id id,
		fqn1.tpe tpe,
		fqn1.sig nme
	FROM
		fqn fqn1 JOIN (SELECT id, max(num) FROM fqn GROUP BY id)  fqn2(id, num)
		ON fqn1.id = fqn2.id AND (fqn1.num = fqn2.num OR fqn1.num IS NULL AND fqn2.num is NULL);

CREATE VIEW sys.describe_privileges AS
	SELECT
		CASE
			WHEN o.tpe IS NULL AND pc.privilege_code_name = 'SELECT' THEN --GLOBAL privileges: SELECT maps to COPY FROM
				'COPY FROM'
			WHEN o.tpe IS NULL AND pc.privilege_code_name = 'UPDATE' THEN --GLOBAL privileges: UPDATE maps to COPY INTO
				'COPY INTO'
			ELSE
				o.nme
		END o_nme,
		CASE
			WHEN o.tpe IS NOT NULL THEN
				o.tpe
			ELSE
				'GLOBAL'
		END o_tpe,
		pc.privilege_code_name p_nme,
		a.name a_nme,
		g.name g_nme,
		p.grantable grantable
	FROM
		sys.privileges p LEFT JOIN
		(
		SELECT t.id, s.name || '.' || t.name , 'TABLE'
			from sys.schemas s, sys.tables t where s.id = t.schema_id
		UNION ALL
			SELECT c.id, s.name || '.' || t.name || '.' || c.name, 'COLUMN'
			FROM sys.schemas s, sys.tables t, sys.columns c where s.id = t.schema_id AND t.id = c.table_id
		UNION ALL
			SELECT f.id, f.nme, f.tpe
			FROM sys.fully_qualified_functions f
		) o(id, nme, tpe) ON o.id = p.obj_id,
		sys.privilege_codes pc,
		auths a, auths g
	WHERE
		p.privileges = pc.privilege_code_id AND
		p.auth_id = a.id AND
		p.grantor = g.id;

CREATE FUNCTION sys.describe_table(schemaName string, tableName string)
  RETURNS TABLE(name string, query string, type string, id integer, remark string)
BEGIN
	RETURN SELECT t.name, t.query, tt.table_type_name, t.id, c.remark
		FROM sys.schemas s, sys.table_types tt, sys._tables t
		LEFT OUTER JOIN sys.comments c ON t.id = c.id
			WHERE s.name = schemaName
			AND t.schema_id = s.id
			AND t.name = tableName
			AND t.type = tt.table_type_id;
END;

CREATE VIEW sys.describe_user_defined_types AS
	SELECT
		s.name sch,
		t.sqlname sql_tpe,
		t.systemname ext_tpe
	FROM sys.types t JOIN sys.schemas s ON t.schema_id = s.id
	WHERE
		t.eclass = 18 AND
		(
			(s.name = 'sys' AND t.sqlname not in ('geometrya', 'mbr', 'url', 'inet', 'json', 'uuid', 'xml')) OR
			(s.name <> 'sys')
		);

CREATE VIEW sys.describe_partition_tables AS
	SELECT 
		m_sch,
		m_tbl,
		p_sch,
		p_tbl,
		CASE
			WHEN p_raw_type IS NULL THEN 'READ ONLY'
			WHEN (p_raw_type = 'VALUES' AND pvalues IS NULL) OR (p_raw_type = 'RANGE' AND minimum IS NULL AND maximum IS NULL AND with_nulls) THEN 'FOR NULLS'
			ELSE p_raw_type
		END AS tpe,
		pvalues,
		minimum,
		maximum,
		with_nulls
	FROM 
    (WITH
		tp("type", table_id) AS
		(SELECT CASE WHEN (table_partitions."type" & 2) = 2 THEN 'VALUES' ELSE 'RANGE' END, table_partitions.table_id FROM sys.table_partitions),
		subq(m_tid, p_mid, "type", m_sch, m_tbl, p_sch, p_tbl) AS
		(SELECT m_t.id, p_m.id, m_t."type", m_s.name, m_t.name, p_s.name, p_m.name
		FROM sys.schemas m_s, sys._tables m_t, sys.dependencies d, sys.schemas p_s, sys._tables p_m
		WHERE m_t."type" IN (3, 6)
			AND m_t.schema_id = m_s.id
			AND m_s.name <> 'tmp'
			AND m_t.system = FALSE
			AND m_t.id = d.depend_id
			AND d.id = p_m.id
			AND p_m.schema_id = p_s.id
		ORDER BY m_t.id, p_m.id)
	SELECT
		subq.m_sch,
		subq.m_tbl,
		subq.p_sch,
		subq.p_tbl,
		tp."type" AS p_raw_type,
		CASE WHEN tp."type" = 'VALUES'
			THEN (SELECT GROUP_CONCAT(vp.value, ',')FROM sys.value_partitions vp WHERE vp.table_id = subq.p_mid)
			ELSE NULL
		END AS pvalues,
		CASE WHEN tp."type" = 'RANGE'
			THEN (SELECT minimum FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)
			ELSE NULL
		END AS minimum,
		CASE WHEN tp."type" = 'RANGE'
			THEN (SELECT maximum FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)
			ELSE NULL
		END AS maximum,
		CASE WHEN tp."type" = 'VALUES'
			THEN EXISTS(SELECT vp.value FROM sys.value_partitions vp WHERE vp.table_id = subq.p_mid AND vp.value IS NULL)
			ELSE (SELECT rp.with_nulls FROM sys.range_partitions rp WHERE rp.table_id = subq.p_mid)
		END AS with_nulls
	FROM 
		subq LEFT OUTER JOIN tp
		ON subq.m_tid = tp.table_id) AS tmp_pi;

CREATE VIEW sys.describe_sequences AS
	SELECT
		s.name as sch,
		seq.name as seq,
		seq."start" s,
		get_value_for(s.name, seq.name) AS rs,
		seq."minvalue" mi,
		seq."maxvalue" ma,
		seq."increment" inc,
		seq."cacheinc" cache,
		seq."cycle" cycle
	FROM sys.sequences seq, sys.schemas s
	WHERE s.id = seq.schema_id
	AND s.name <> 'tmp'
	ORDER BY s.name, seq.name;

CREATE VIEW sys.describe_functions AS
	SELECT
		f.id o,
		s.name sch,
		f.name fun,
		f.func def
	FROM sys.functions f JOIN sys.schemas s ON f.schema_id = s.id WHERE s.name <> 'tmp' AND NOT f.system;

CREATE FUNCTION sys.describe_columns(schemaName string, tableName string)
	RETURNS TABLE(name string, type string, digits integer, scale integer, Nulls boolean, cDefault string, number integer, sqltype string, remark string)
BEGIN
	RETURN SELECT c.name, c."type", c.type_digits, c.type_scale, c."null", c."default", c.number, sys.describe_type(c."type", c.type_digits, c.type_scale), com.remark
		FROM sys._tables t, sys.schemas s, sys._columns c
		LEFT OUTER JOIN sys.comments com ON c.id = com.id
			WHERE c.table_id = t.id
			AND t.name = tableName
			AND t.schema_id = s.id
			AND s.name = schemaName
		ORDER BY c.number;
END;

CREATE FUNCTION sys.describe_function(schemaName string, functionName string)
	RETURNS TABLE(id integer, name string, type string, language string, remark string)
BEGIN
    RETURN SELECT f.id, f.name, ft.function_type_keyword, fl.language_keyword, c.remark
        FROM sys.functions f
        JOIN sys.schemas s ON f.schema_id = s.id
        JOIN sys.function_types ft ON f.type = ft.function_type_id
        LEFT OUTER JOIN sys.function_languages fl ON f.language = fl.language_id
        LEFT OUTER JOIN sys.comments c ON f.id = c.id
        WHERE f.name=functionName AND s.name = schemaName;
END;
