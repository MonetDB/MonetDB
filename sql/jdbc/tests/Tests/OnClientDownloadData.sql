-- test ON CLIENT data exports with different delimiters
COPY SELECT * FROM sys.tables ORDER BY id LIMIT 121 INTO 'sys_tables_by_id.dsv' ON CLIENT;
COPY SELECT * FROM sys.tables ORDER BY id LIMIT 122 INTO 'sys_tables_by_id.csv' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
COPY SELECT * FROM sys.tables ORDER BY id LIMIT 123 INTO 'sys_tables_by_id.tsv' ON CLIENT USING DELIMITERS E'\t' , E'\n' , '"' NULL AS '';
COPY SELECT * FROM sys.tables ORDER BY id LIMIT 124 INTO 'sys_tables_by_id.psv' ON CLIENT USING DELIMITERS '|' , E'\n' , '"' NULL AS '';

-- and compressed exports (only .gz is supported)
COPY SELECT * FROM sys.tables ORDER BY id LIMIT 120 INTO 'sys_tables_by_id.csv.gz' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';


-- test error handling
COPY SELECT * FROM sys.tables ORDER BY id INTO '' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
-- Error [22000] Missing file name

-- next tests are disabled due to variable path in error msg
--COPY SELECT * FROM sys.tables ORDER BY id INTO '.' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
-- Error [22000] File already exists: /home/dinther/csvfiles
--COPY SELECT * FROM sys.tables ORDER BY id INTO '..' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
-- Error [22000] File is not in download directory: /home/dinther/csvfiles
--COPY SELECT * FROM sys.tables ORDER BY id INTO '../b' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
-- Error [22000] File is not in download directory: /home/dinther/csvfiles
--COPY SELECT * FROM sys.tables ORDER BY id INTO '/home/Doesnotexist/sys_tables_by_id.dsv' ON CLIENT;
-- Error [22000] File is not in download directory: /home/dinther/csvfiles

--COPY SELECT * FROM sys.tables ORDER BY id INTO 'sys_tables_by_id.dsv' ON CLIENT;
-- Error [22000] File already exists: /home/dinther/csvfiles/sys_tables_by_id.dsv
--COPY SELECT * FROM sys.tables ORDER BY id INTO 'sys_tables_by_id.csv.gz' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
-- Error [22000] File already exists: /home/dinther/csvfiles/sys_tables_by_id.csv.gz

COPY SELECT * FROM sys.tables ORDER BY id INTO 'sys_tables_by_id.csv.bz2' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
-- Error [22000] Requested compression .bz2 is not supported. Use .gz instead.
COPY SELECT * FROM sys.tables ORDER BY id INTO 'sys_tables_by_id.csv.lz4' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
-- Error [22000] Requested compression .lz4 is not supported. Use .gz instead.
COPY SELECT * FROM sys.tables ORDER BY id INTO 'sys_tables_by_id.csv.xz' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
-- Error [22000] Requested compression .xz is not supported. Use .gz instead.
COPY SELECT * FROM sys.tables ORDER BY id INTO 'sys_tables_by_id.csv.zip' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
-- Error [22000] Requested compression .zip is not supported. Use .gz instead.

