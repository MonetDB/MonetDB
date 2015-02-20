-- next query synthesizes the queries for SQL systems table column names which are also reserved words except for
-- 'default', 'null', 'unique' and 'user' as those names need to be avoided as SQL systems table column names at all times.
SELECT 'SELECT DISTINCT '||C.name||' FROM '||S.name||'.'||T.name||' WHERE '||C.name||' IS NOT NULL ORDER BY '||C.name||';' as SQL_query
  FROM sys.columns C join sys.tables T on C.table_id = T.id join sys.schemas S on T.schema_id = S.id
 WHERE lower(C.name) in ('action', 'as', 'authorization', 'column', 'cycle', 'distinct', 'increment', 'maxvalue', 'minvalue', 'plan', 'sample', 'schema', 'start', 'statement', 'table')
 ORDER BY C.name, S.name, T.name;

SELECT DISTINCT action FROM sys.keys WHERE action IS NOT NULL ORDER BY action;
SELECT DISTINCT action FROM tmp.keys WHERE action IS NOT NULL ORDER BY action;
SELECT DISTINCT as FROM bam.sq WHERE as IS NOT NULL ORDER BY as;
SELECT DISTINCT authorization FROM sys.schemas WHERE authorization IS NOT NULL ORDER BY authorization;
SELECT DISTINCT column FROM sys.storage WHERE column IS NOT NULL ORDER BY column;
SELECT DISTINCT column FROM sys.storagemodel WHERE column IS NOT NULL ORDER BY column;
SELECT DISTINCT column FROM sys.storagemodelinput WHERE column IS NOT NULL ORDER BY column;
SELECT DISTINCT cycle FROM sys.sequences WHERE cycle IS NOT NULL ORDER BY cycle;
SELECT DISTINCT distinct FROM sys.storagemodelinput WHERE distinct IS NOT NULL ORDER BY distinct;
SELECT DISTINCT increment FROM sys.sequences WHERE increment IS NOT NULL ORDER BY increment;
SELECT DISTINCT maxvalue FROM sys.sequences WHERE maxvalue IS NOT NULL ORDER BY maxvalue;
SELECT DISTINCT minvalue FROM sys.sequences WHERE minvalue IS NOT NULL ORDER BY minvalue;
SELECT DISTINCT plan FROM sys.querylog_catalog WHERE plan IS NOT NULL ORDER BY plan;
SELECT DISTINCT plan FROM sys.querylog_history WHERE plan IS NOT NULL ORDER BY plan;
SELECT DISTINCT sample FROM sys.statistics WHERE sample IS NOT NULL ORDER BY sample;
SELECT DISTINCT schema FROM sys.storage WHERE schema IS NOT NULL ORDER BY schema;
SELECT DISTINCT schema FROM sys.storagemodel WHERE schema IS NOT NULL ORDER BY schema;
SELECT DISTINCT schema FROM sys.storagemodelinput WHERE schema IS NOT NULL ORDER BY schema;
SELECT DISTINCT schema FROM sys.tablestoragemodel WHERE schema IS NOT NULL ORDER BY schema;
SELECT DISTINCT start FROM sys.querylog_calls WHERE start IS NOT NULL ORDER BY start;
SELECT DISTINCT start FROM sys.querylog_history WHERE start IS NOT NULL ORDER BY start;
SELECT DISTINCT start FROM sys.sequences WHERE start IS NOT NULL ORDER BY start;
SELECT DISTINCT statement FROM sys.triggers WHERE statement IS NOT NULL ORDER BY statement;
SELECT DISTINCT statement FROM tmp.triggers WHERE statement IS NOT NULL ORDER BY statement;
SELECT DISTINCT table FROM sys.storage WHERE table IS NOT NULL ORDER BY table;
SELECT DISTINCT table FROM sys.storagemodel WHERE table IS NOT NULL ORDER BY table;
SELECT DISTINCT table FROM sys.storagemodelinput WHERE table IS NOT NULL ORDER BY table;
SELECT DISTINCT table FROM sys.tablestoragemodel WHERE table IS NOT NULL ORDER BY table;

