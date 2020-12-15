-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

CREATE FUNCTION sys.describe_type(ctype string, digits integer, tscale integer)
  RETURNS string
BEGIN
  RETURN
    CASE ctype
      WHEN 'bigint' THEN 'BIGINT'
      WHEN 'blob' THEN
	CASE digits
	  WHEN 0 THEN 'BINARY LARGE OBJECT'
	  ELSE 'BINARY LARGE OBJECT(' || CAST(digits AS string) || ')'
	END
      WHEN 'boolean' THEN 'BOOLEAN'
      WHEN 'char' THEN
        CASE digits
          WHEN 1 THEN 'CHARACTER'
          ELSE 'CHARACTER(' || CAST(digits AS string) || ')'
        END
      WHEN 'clob' THEN
	CASE digits
	  WHEN 0 THEN 'CHARACTER LARGE OBJECT'
	  ELSE 'CHARACTER LARGE OBJECT(' || CAST(digits AS string) || ')'
	END
      WHEN 'date' THEN 'DATE'
      WHEN 'day_interval' THEN 'INTERVAL DAY'
      WHEN 'decimal' THEN 'DECIMAL(' || CAST(digits AS string) || ',' || CAST(tscale AS string) || ')'
      WHEN 'double' THEN
	CASE
	  WHEN digits = 53 and tscale = 0 THEN 'DOUBLE'
	  WHEN tscale = 0 THEN 'FLOAT(' || CAST(digits AS string) || ')'
	  ELSE 'FLOAT(' || CAST(digits AS string) || ',' || CAST(tscale AS string) || ')'
	END
      WHEN 'geometry' THEN
	CASE digits
	  WHEN 4 THEN 'GEOMETRY(POINT' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || CAST(tscale AS string)
            END || ')'
	  WHEN 8 THEN 'GEOMETRY(LINESTRING' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || CAST(tscale AS string)
            END || ')'
	  WHEN 16 THEN 'GEOMETRY(POLYGON' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || CAST(tscale AS string)
            END || ')'
	  WHEN 20 THEN 'GEOMETRY(MULTIPOINT' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || CAST(tscale AS string)
            END || ')'
	  WHEN 24 THEN 'GEOMETRY(MULTILINESTRING' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || CAST(tscale AS string)
            END || ')'
	  WHEN 28 THEN 'GEOMETRY(MULTIPOLYGON' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || CAST(tscale AS string)
            END || ')'
	  WHEN 32 THEN 'GEOMETRY(GEOMETRYCOLLECTION' ||
            CASE tscale
              WHEN 0 THEN ''
              ELSE ',' || CAST(tscale AS string)
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
	  WHEN tscale = 0 THEN 'FLOAT(' || CAST(digits AS string) || ')'
	  ELSE 'FLOAT(' || CAST(digits AS string) || ',' || CAST(tscale AS string) || ')'
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
	  ELSE 'TIME(' || CAST(digits - 1 AS string) || ')'
	END
      WHEN 'timestamp' THEN
	CASE digits
	  WHEN 7 THEN 'TIMESTAMP'
	  ELSE 'TIMESTAMP(' || CAST(digits - 1 AS string) || ')'
	END
      WHEN 'timestamptz' THEN
	CASE digits
	  WHEN 7 THEN 'TIMESTAMP'
	  ELSE 'TIMESTAMP(' || CAST(digits - 1 AS string) || ')'
	END || ' WITH TIME ZONE'
      WHEN 'timetz' THEN
	CASE digits
	  WHEN 1 THEN 'TIME'
	  ELSE 'TIME(' || CAST(digits - 1 AS string) || ')'
	END || ' WITH TIME ZONE'
      WHEN 'tinyint' THEN 'TINYINT'
      WHEN 'varchar' THEN 'CHARACTER VARYING(' || CAST(digits AS string) || ')'
      ELSE
        CASE
          WHEN lower(ctype) = ctype THEN upper(ctype)
          ELSE '"' || ctype || '"'
        END || CASE digits
	  WHEN 0 THEN ''
          ELSE '(' || CAST(digits AS string) || CASE tscale
	    WHEN 0 THEN ''
            ELSE ',' || CAST(tscale AS string)
          END || ')'
	END
    END;
END;

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

CREATE FUNCTION sys.describe_columns(schemaName string, tableName string)
	RETURNS TABLE(name string, type string, digits integer, scale integer, Nulls boolean, cDefault string, number integer, sqltype string, remark string)
BEGIN
	RETURN SELECT c.name, c."type", c.type_digits, c.type_scale, c."null", c."default", c.number, describe_type(c."type", c.type_digits, c.type_scale), com.remark
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

CREATE FUNCTION fully_qualified_functions() RETURNS TABLE(id INT, tpe STRING, nme STRING) BEGIN
RETURN
	WITH fqn(id, tpe, sig, num) AS
	(
		SELECT
			f.id,
			ft.function_type_keyword,
			CASE WHEN a.type IS NULL THEN
				s.name || '.' || f.name || '()'
			ELSE
				s.name || '.' || f.name || '(' || group_concat(describe_type(a.type, a.type_digits, a.type_scale), ',') OVER (PARTITION BY f.id ORDER BY a.number)  || ')'
			END,
			a.number
		FROM schemas s, sys.function_types ft, functions f LEFT JOIN args a ON f.id = a.func_id
		WHERE s.id= f.schema_id AND f.type = ft.function_type_id
	)
	SELECT
		fqn1.id,
		fqn1.tpe,
		fqn1.sig
	FROM
		fqn fqn1 JOIN (SELECT id, max(num) FROM fqn GROUP BY id)  fqn2(id, num)
		ON fqn1.id = fqn2.id AND (fqn1.num = fqn2.num OR fqn1.num IS NULL AND fqn2.num is NULL);
END;
