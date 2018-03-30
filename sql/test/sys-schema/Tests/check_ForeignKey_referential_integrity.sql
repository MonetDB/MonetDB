-- Check all standard sys (and tmp) tables on Referential Integrity
-- All queries should return NO rows (so no invalid references found).
SELECT * FROM sys.schemas WHERE authorization NOT IN (SELECT id FROM sys.auths);
SELECT * FROM sys.schemas WHERE owner NOT IN (SELECT id FROM sys.auths);

SELECT * FROM sys.tables WHERE schema_id NOT IN (SELECT id FROM sys.schemas);
SELECT * FROM sys._tables WHERE schema_id NOT IN (SELECT id FROM sys.schemas);
SELECT * FROM tmp._tables WHERE schema_id NOT IN (SELECT id FROM sys.schemas);
SELECT * FROM sys.tables WHERE type NOT IN (SELECT table_type_id FROM sys.table_types);
SELECT * FROM sys._tables WHERE type NOT IN (SELECT table_type_id FROM sys.table_types);
SELECT * FROM tmp._tables WHERE type NOT IN (SELECT table_type_id FROM sys.table_types);

SELECT * FROM sys.columns WHERE table_id NOT IN (SELECT id FROM sys.tables);
SELECT * FROM sys._columns WHERE table_id NOT IN (SELECT id FROM sys._tables);
SELECT * FROM tmp._columns WHERE table_id NOT IN (SELECT id FROM tmp._tables);
SELECT * FROM sys.columns WHERE type NOT IN (SELECT sqlname FROM sys.types);
SELECT * FROM sys._columns WHERE type NOT IN (SELECT sqlname FROM sys.types);
SELECT * FROM sys._columns WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT * FROM sys.functions WHERE schema_id NOT IN (SELECT id FROM sys.schemas);
SELECT * FROM sys.functions WHERE type NOT IN (SELECT function_type_id FROM sys.function_types);
-- SELECT * FROM sys.functions WHERE type NOT IN (1,2,3,4,5,6,7);  -- old check before table sys.function_types existed
SELECT * FROM sys.functions WHERE language NOT IN (SELECT language_id FROM sys.function_languages);
-- SELECT * FROM sys.functions WHERE language NOT IN (0,1,2,3,4,5,6,7);  -- old check before table sys.function_languages existed

SELECT * FROM sys.systemfunctions WHERE function_id NOT IN (SELECT id FROM sys.functions);
-- systemfunctions should refer only to functions in MonetDB system schemas (on Dec2016 these are: sys, json, profiler and bam)
SELECT * FROM sys.systemfunctions WHERE function_id NOT IN (SELECT id FROM sys.functions WHERE schema_id IN (SELECT id FROM sys.schemas WHERE name IN ('sys','json','profiler','bam')));

SELECT * FROM sys.args WHERE func_id NOT IN (SELECT id FROM sys.functions);
SELECT * FROM sys.args WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT * FROM sys.types WHERE schema_id NOT IN (SELECT id FROM sys.schemas);
SELECT * FROM sys.types WHERE schema_id NOT IN (SELECT id FROM sys.schemas) AND schema_id <> 0;

SELECT * FROM sys.objects WHERE id NOT IN (SELECT id FROM sys.ids);
SELECT * FROM sys.objects WHERE id NOT IN (SELECT id FROM sys.ids);
SELECT * FROM sys.ids WHERE obj_type IN ('key', 'index') AND id NOT IN (SELECT id FROM sys.objects);

SELECT * FROM sys.keys WHERE id NOT IN (SELECT id FROM sys.objects);
SELECT * FROM sys.keys WHERE table_id NOT IN (SELECT id FROM sys.tables);
SELECT * FROM sys.keys WHERE table_id NOT IN (SELECT id FROM sys._tables);
SELECT * FROM tmp.keys WHERE table_id NOT IN (SELECT id FROM tmp._tables);
SELECT * FROM sys.keys WHERE type NOT IN (SELECT key_type_id FROM sys.key_types);
SELECT * FROM tmp.keys WHERE type NOT IN (SELECT key_type_id FROM sys.key_types);
-- SELECT * FROM sys.keys WHERE type NOT IN (0, 1, 2);  -- old check before table sys.key_types existed
-- SELECT * FROM tmp.keys WHERE type NOT IN (0, 1, 2);  -- old check before table sys.key_types existed
SELECT * FROM sys.keys WHERE rkey <> -1 AND rkey NOT IN (SELECT id FROM sys.keys);
SELECT * FROM tmp.keys WHERE rkey <> -1 AND rkey NOT IN (SELECT id FROM tmp.keys);
-- SELECT * FROM sys.keys WHERE action <> -1 AND action NOT IN (SELECT id FROM sys.?);  -- TODO: find out which action values are valid and what they mean.
-- SELECT * FROM tmp.keys WHERE action <> -1 AND action NOT IN (SELECT id FROM tmp.?);  -- TODO: find out which action values are valid and what they mean.

SELECT * FROM sys.idxs WHERE id NOT IN (SELECT id FROM sys.objects);
SELECT * FROM sys.idxs WHERE table_id NOT IN (SELECT id FROM sys.tables);
SELECT * FROM sys.idxs WHERE table_id NOT IN (SELECT id FROM sys._tables);
SELECT * FROM tmp.idxs WHERE table_id NOT IN (SELECT id FROM tmp._tables);
SELECT * FROM sys.idxs WHERE type NOT IN (SELECT index_type_id FROM sys.index_types);
SELECT * FROM tmp.idxs WHERE type NOT IN (SELECT index_type_id FROM sys.index_types);
-- SELECT * FROM sys.idxs WHERE type NOT IN (0, 1, 2);  -- old check before table sys.index_types existed
-- SELECT * FROM tmp.idxs WHERE type NOT IN (0, 1, 2);  -- old check before table sys.index_types existed

SELECT * FROM sys.sequences WHERE schema_id NOT IN (SELECT id FROM sys.schemas);

SELECT * FROM sys.triggers WHERE table_id NOT IN (SELECT id FROM sys.tables);
SELECT * FROM sys.triggers WHERE table_id NOT IN (SELECT id FROM sys._tables);
SELECT * FROM tmp.triggers WHERE table_id NOT IN (SELECT id FROM tmp._tables);

SELECT * FROM sys.comments WHERE id NOT IN (SELECT id FROM sys.ids);

SELECT * FROM sys.dependencies WHERE id NOT IN (SELECT id FROM sys.ids);
SELECT * FROM sys.dependencies WHERE depend_id NOT IN (SELECT id FROM sys.ids);
SELECT * FROM sys.dependencies WHERE depend_type NOT IN (SELECT dependency_type_id FROM sys.dependency_types);
SELECT * FROM sys.dependencies WHERE (id, depend_id, depend_type) NOT IN (SELECT v.id, v.used_by_id, v.depend_type FROM sys.dependencies_vw v);

SELECT * FROM sys.auths WHERE grantor NOT IN (SELECT id FROM sys.auths) AND grantor > 0;
SELECT * FROM sys.users WHERE name NOT IN (SELECT name FROM sys.auths);
SELECT * FROM sys.users WHERE default_schema NOT IN (SELECT id FROM sys.schemas);
SELECT * FROM sys.db_user_info WHERE name NOT IN (SELECT name FROM sys.auths);
SELECT * FROM sys.db_user_info WHERE default_schema NOT IN (SELECT id FROM sys.schemas);

SELECT * FROM sys.user_role WHERE login_id NOT IN (SELECT id FROM sys.auths);
SELECT * FROM sys.user_role WHERE login_id NOT IN (SELECT a.id FROM sys.auths a WHERE a.name IN (SELECT u.name FROM sys.users u));
SELECT * FROM sys.user_role WHERE role_id NOT IN (SELECT id FROM sys.auths);
SELECT * FROM sys.user_role WHERE role_id NOT IN (SELECT a.id FROM sys.auths a WHERE a.name NOT IN (SELECT u.name FROM sys.users u));
SELECT * FROM sys.user_role WHERE role_id NOT IN (SELECT id FROM sys.roles);

SELECT * FROM sys.privileges WHERE obj_id NOT IN (SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys._tables UNION ALL SELECT id FROM sys._columns UNION ALL SELECT id FROM sys.functions);
SELECT * FROM sys.privileges WHERE auth_id NOT IN (SELECT id FROM sys.auths);
SELECT * FROM sys.privileges WHERE grantor NOT IN (SELECT id FROM sys.auths) AND grantor > 0;
SELECT * FROM sys.privileges WHERE privileges NOT IN (SELECT privilege_code_id FROM sys.privilege_codes);
--SELECT * FROM sys.privileges WHERE privileges NOT IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,32); -- old check before table sys.privilege_codes existed

SELECT * FROM sys.querylog_catalog WHERE owner NOT IN (SELECT name FROM sys.users);
SELECT * FROM sys.querylog_catalog WHERE pipe NOT IN (SELECT name FROM sys.optimizers);
SELECT * FROM sys.querylog_calls   WHERE id NOT IN (SELECT id FROM sys.querylog_catalog);
SELECT * FROM sys.querylog_history WHERE id NOT IN (SELECT id FROM sys.querylog_catalog);
SELECT * FROM sys.querylog_history WHERE owner NOT IN (SELECT name FROM sys.users);
SELECT * FROM sys.querylog_history WHERE pipe NOT IN (SELECT name FROM sys.optimizers);

SELECT * FROM sys.queue WHERE tag > cast(0 as oid) AND tag NOT IN (SELECT cast(qtag as oid) FROM sys.queue);
SELECT * FROM sys.queue WHERE "user" NOT IN (SELECT name FROM sys.users);

SELECT * FROM sys.sessions WHERE "user" NOT IN (SELECT name FROM sys.users);

SELECT * FROM sys.statistics WHERE column_id NOT IN (SELECT id FROM sys._columns UNION ALL SELECT id FROM tmp._columns);
SELECT * FROM sys.statistics WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT * FROM sys.storage WHERE schema NOT IN (SELECT name FROM sys.schemas);
SELECT * FROM sys.storage WHERE table NOT IN (SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables);
SELECT * FROM sys.storage WHERE column NOT IN (SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys);
SELECT * FROM sys.storage WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT * FROM sys.storagemodel WHERE schema NOT IN (SELECT name FROM sys.schemas);
SELECT * FROM sys.storagemodel WHERE table NOT IN (SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables);
SELECT * FROM sys.storagemodel WHERE column NOT IN (SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys);
SELECT * FROM sys.storagemodel WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT * FROM sys.storagemodelinput WHERE schema NOT IN (SELECT name FROM sys.schemas);
SELECT * FROM sys.storagemodelinput WHERE table NOT IN (SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables);
SELECT * FROM sys.storagemodelinput WHERE column NOT IN (SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys);
SELECT * FROM sys.storagemodelinput WHERE type NOT IN (SELECT sqlname FROM sys.types);

SELECT schema, table, count, columnsize, heapsize, hashes, imprints, cast(auxiliary as bigint) as auxiliary FROM sys.tablestoragemodel WHERE schema NOT IN (SELECT name FROM sys.schemas);
SELECT schema, table, count, columnsize, heapsize, hashes, imprints, cast(auxiliary as bigint) as auxiliary FROM sys.tablestoragemodel WHERE table NOT IN (SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables);

