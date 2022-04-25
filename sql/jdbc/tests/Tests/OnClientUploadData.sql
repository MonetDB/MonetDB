DROP TABLE IF EXISTS importedFileData;
CREATE TABLE importedFileData AS SELECT * FROM sys.tables WITH NO DATA;

COPY INTO importedFileData FROM 'sys_tables_by_id.dsv' ON CLIENT USING DELIMITERS '|' , E'\n' , '"' NULL AS '';
SELECT COUNT(*) > 0 FROM importedFileData;
TRUNCATE importedFileData;

COPY INTO importedFileData FROM 'sys_tables_by_id.csv' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
SELECT COUNT(*) > 0 FROM importedFileData;
TRUNCATE importedFileData;

COPY 20 OFFSET 5 RECORDS INTO importedFileData FROM 'sys_tables_by_id.csv' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
SELECT COUNT(*) > 0 FROM importedFileData;
TRUNCATE importedFileData;

COPY INTO importedFileData FROM 'sys_tables_by_id.tsv' ON CLIENT USING DELIMITERS E'\t' , E'\n' , '"' NULL AS '';
SELECT COUNT(*) > 0 FROM importedFileData;
TRUNCATE importedFileData;

COPY INTO importedFileData FROM 'sys_tables_by_id.psv' ON CLIENT USING DELIMITERS '|' , E'\n' , '"' NULL AS '';
SELECT COUNT(*) > 0 FROM importedFileData;
TRUNCATE importedFileData;

-- compressed .gz imports
COPY INTO importedFileData FROM 'sys_tables_by_id.csv.gz' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
SELECT COUNT(*) > 0 FROM importedFileData;
TRUNCATE importedFileData;

COPY 80 OFFSET 5 RECORDS INTO importedFileData FROM 'sys_tables_by_id.csv.gz' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
SELECT COUNT(*) > 0 FROM importedFileData;
TRUNCATE importedFileData;


-- other compression formats are NOT supported
-- next tests are disabled due to variable path in error msg
--COPY INTO importedFileData FROM 'sys_tables_by_id.csv.bz2' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
--COPY INTO importedFileData FROM 'sys_tables_by_id.csv.lz4' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
--COPY INTO importedFileData FROM 'sys_tables_by_id.csv.xz' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';
--COPY INTO importedFileData FROM 'sys_tables_by_id.csv.zip' ON CLIENT USING DELIMITERS ',' , E'\n' , '"' NULL AS '';

DROP TABLE IF EXISTS importedFileData;

