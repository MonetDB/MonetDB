-- check all standard MonetDB sys (and tmp) tables on Primary Key uniqueness.
-- All queries should return NO rows (so no duplicates found).

SELECT COUNT(*) AS duplicates, id FROM sys.schemas GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, table_type_id FROM sys.table_types GROUP BY table_type_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys._tables GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM tmp._tables GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys.tables GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys._columns GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM tmp._columns GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys.columns GROUP BY id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, function_type_id FROM sys.function_types GROUP BY function_type_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, language_id FROM sys.function_languages GROUP BY language_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys.functions GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, function_id FROM sys.systemfunctions GROUP BY function_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys.args GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys.types GROUP BY id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, id, nr FROM sys.objects GROUP BY id, nr HAVING COUNT(*) > 1;  -- without column: nr it returns duplicates
SELECT COUNT(*) AS duplicates, id, nr FROM tmp.objects GROUP BY id, nr HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, key_type_id FROM sys.key_types GROUP BY key_type_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys.keys GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM tmp.keys GROUP BY id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, index_type_id FROM sys.index_types GROUP BY index_type_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys.idxs GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM tmp.idxs GROUP BY id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, id FROM sys.triggers GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM tmp.triggers GROUP BY id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, id FROM sys.sequences GROUP BY id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, id FROM sys.comments GROUP BY id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, dependency_type_id FROM sys.dependency_types GROUP BY dependency_type_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id, depend_id FROM sys.dependencies GROUP BY id, depend_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys.ids GROUP BY id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, id FROM sys.auths GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, name FROM sys.users GROUP BY name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, login_id, role_id FROM sys.user_role GROUP BY login_id, role_id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, privilege_code_id FROM sys.privilege_codes GROUP BY privilege_code_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, obj_id, auth_id FROM sys.privileges GROUP BY obj_id, auth_id HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, id FROM sys.querylog_catalog GROUP BY id HAVING COUNT(*) >1;
SELECT COUNT(*) AS duplicates, id FROM sys.querylog_calls GROUP BY id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, id FROM sys.querylog_history GROUP BY id HAVING COUNT(*) >1;
SELECT COUNT(*) AS duplicates, tag FROM sys.queue GROUP BY tag HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, name FROM sys.optimizers GROUP BY name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, name FROM sys.environment GROUP BY name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, keyword FROM sys.keywords GROUP BY keyword HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, name FROM sys.db_user_info GROUP BY name HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, "username", login FROM sys.sessions GROUP BY "username", login HAVING COUNT(*) > 1;  -- is this really always unique?

SELECT COUNT(*) AS duplicates, column_id FROM sys.statistics GROUP BY column_id HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, rowid FROM sys.rejects GROUP BY rowid HAVING COUNT(*) > 1;

SELECT COUNT(*) AS duplicates, schema FROM sys.schemastorage GROUP BY schema HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, schema, table, column FROM sys.storage GROUP BY schema, table, column HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, schema, table, column FROM sys.storagemodel GROUP BY schema, table, column HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, schema, table, column FROM sys.storagemodelinput GROUP BY schema, table, column HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, schema, table FROM sys.tablestorage GROUP BY schema, table HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, schema, table FROM sys.tablestoragemodel GROUP BY schema, table HAVING COUNT(*) > 1;

-- new tables introduced in 2019
SELECT COUNT(*) AS duplicates, id FROM sys.table_partitions GROUP BY id HAVING COUNT(*) >1;
SELECT COUNT(*) AS duplicates, table_id, partition_id, minimum FROM sys.range_partitions GROUP BY table_id, partition_id, minimum HAVING COUNT(*) >1;
SELECT COUNT(*) AS duplicates, table_id, partition_id, "value" FROM sys.value_partitions GROUP BY table_id, partition_id, "value" HAVING COUNT(*) >1;

--SELECT COUNT(*) AS duplicates, event FROM sys.tracelog GROUP BY event HAVING COUNT(*) > 1;  -- Error: Profiler not started

-- NOT included here are the 5 netcdf_* tables and GEOM table spatial_ref_sys and bam schema tables as those aren't available on all platforms.

