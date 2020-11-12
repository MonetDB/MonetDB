

create function describe_table(schemaName string, tableName string)
	returns table(name string, query string, type string, id integer, remark string)
BEGIN
	return SELECT t.name, t.query, 
			CASE 
				WHEN t.type = 0 THEN 'TABLE' 
				WHEN t.type = 1 THEN 'VIEW' 
				WHEN t.type = 3 THEN 'MERGE TABLE' 
				WHEN t.type = 4 THEN 'STREAM TABLE' 
				WHEN t.type = 5 THEN 'REMOTE TABLE' 
				ELSE 'REPLICA TABLE' 
			END, t.id, c.remark 
         	FROM sys.schemas s, sys._tables t 
            	LEFT OUTER JOIN sys.comments c ON t.id = c.id 
         		WHERE s.name = schemaName
           		AND t.schema_id = s.id 
           		AND t.name = tableName;
END;

create function describe_columns(schemaName string, tableName string)
	returns table(name string, type string, digits integer, scale integer, Nulls boolean, cDefault string, number integer, remark string)
BEGIN
	return SELECT c.name, c."type", c.type_digits, c.type_scale, c."null", c."default", c.number, com.remark 
             FROM sys._tables t, sys.schemas s, sys._columns c 
	LEFT OUTER JOIN sys.comments com ON c.id = com.id 
             WHERE c.table_id = t.id AND t.name = tableName AND t.schema_id = s.id AND s.name = schemaName
             ORDER BY c.number;
END;
