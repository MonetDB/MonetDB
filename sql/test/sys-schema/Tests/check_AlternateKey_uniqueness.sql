-- check all standard sys (and tmp) tables on Alternate Key uniqueness
-- All queries should return NO rows (so no duplicates found).

SELECT COUNT(*) AS duplicates, name FROM sys.schemas GROUP BY name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_type_name FROM sys.table_types GROUP BY table_type_name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, schema_id, name FROM sys._tables GROUP BY schema_id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, schema_id, name FROM tmp._tables GROUP BY schema_id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, schema_id, name FROM sys.tables GROUP BY schema_id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, name FROM sys._columns GROUP BY table_id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, name FROM tmp._columns GROUP BY table_id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, name FROM sys.columns GROUP BY table_id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, number FROM sys._columns GROUP BY table_id, number HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, number FROM tmp._columns GROUP BY table_id, number HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, number FROM sys.columns GROUP BY table_id, number HAVING COUNT(*) > 1;

-- The id values from sys.schemas, sys._tables, sys._columns and sys.functions combined must be exclusive (see FK from sys.privileges.obj_id)
SELECT COUNT(*) AS duplicates, T.id FROM (SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys._tables UNION ALL SELECT id FROM sys._columns UNION ALL SELECT id FROM sys.functions) T GROUP BY T.id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, T.id FROM (SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys.tables UNION ALL SELECT id FROM sys.columns UNION ALL SELECT id FROM sys.functions) T GROUP BY T.id HAVING COUNT(*) > 1;

-- the next query returns duplicates for overloaded functions (same function but with different args), hence it has been disabled
--SELECT COUNT(*) AS duplicates, schema_id, name, func, mod, language, type, side_effect, varres, vararg FROM sys.functions
-- GROUP BY schema_id, name, func, mod, language, type, side_effect, varres, vararg HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, func_id, name FROM sys.args GROUP BY func_id, name HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, schema_id, systemname, sqlname FROM sys.types GROUP BY schema_id, systemname, sqlname HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, id, name FROM sys.objects GROUP BY id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id, name FROM tmp.objects GROUP BY id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, name FROM sys.keys GROUP BY table_id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, name FROM tmp.keys GROUP BY table_id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, name FROM sys.idxs GROUP BY table_id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, name FROM tmp.idxs GROUP BY table_id, name HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, table_id, name FROM sys.triggers GROUP BY table_id, name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_id, name FROM tmp.triggers GROUP BY table_id, name HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, schema_id, name FROM sys.sequences GROUP BY schema_id, name HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, dependency_type_name FROM sys.dependency_types GROUP BY dependency_type_name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, function_type_name FROM sys.function_types GROUP BY function_type_name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, language_name FROM sys.function_languages GROUP BY language_name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, key_type_name FROM sys.key_types GROUP BY key_type_name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, index_type_name FROM sys.index_types GROUP BY index_type_name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, privilege_code_name FROM sys.privilege_codes GROUP BY privilege_code_name HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, name FROM sys.auths GROUP BY name HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, def FROM sys.optimizers GROUP BY def HAVING COUNT(*) > 1;

-- new tables introduced in 2019
SELECT COUNT(*) AS duplicates, table_id, column_id FROM sys.table_partitions WHERE "column_id" IS NOT NULL GROUP BY table_id, column_id HAVING COUNT(*) >1;
SELECT COUNT(*) AS duplicates, table_id, "expression" FROM sys.table_partitions WHERE "expression" IS NOT NULL GROUP BY table_id, "expression" HAVING COUNT(*) >1;
SELECT COUNT(*) AS duplicates, table_id, partition_id, "maximum" FROM sys.range_partitions GROUP BY table_id, partition_id, "maximum" HAVING COUNT(*) >1;
SELECT COUNT(*) AS duplicates, table_id, partition_id, "minimum" FROM sys.range_partitions GROUP BY table_id, partition_id, "minimum"  HAVING COUNT(*) >1;

