-- First check that no invalid FK references exist in the db. All next queries should return zero rows:
SELECT * FROM sys.columns WHERE table_id NOT IN (SELECT id FROM sys.tables);
SELECT * FROM sys._columns WHERE table_id NOT IN (SELECT id FROM sys._tables);
SELECT * FROM sys.keys WHERE table_id NOT IN (SELECT id FROM sys.tables);
SELECT * FROM sys.objects WHERE id NOT IN (SELECT id FROM sys.ids);
SELECT * FROM sys.idxs WHERE table_id NOT IN (SELECT id FROM sys.tables);
SELECT * FROM sys.dependencies WHERE id NOT IN (SELECT id FROM sys.ids);
SELECT * FROM sys.dependencies WHERE depend_id NOT IN (SELECT id FROM sys.ids);

-- create a table with a serial column (which implicitly createa a primary key constraint and index and a sequence)
create table sys.test2 (col1 serial);
select count(*) as count_rows from tables where name = 'test2';
-- 1 row: test2  (id = 9046)
select count(*) as count_rows from keys where table_id in (select id from tables where name = 'test2');
-- 1 row: test2_col1_pkey
select count(*) as count_rows from objects where id in (select id from keys where table_id in (select id from tables where name = 'test2'));
-- 1 row: col1

ALTER TABLE sys.test2 SET SCHEMA profiler;
select count(*) as count_rows from tables where name = 'test2';
-- 1 row: test2. NOTE that the id has changed from 9046 into 9048
select count(*) as count_rows from keys where table_id in (select id from tables where name = 'test2');
-- 1 row: test2_col1_pkey
select count(*) as count_rows from objects where id in (select id from keys where table_id in (select id from tables where name = 'test2'));
-- 2 rows! which are also identical: col1

ALTER TABLE profiler.test2 SET SCHEMA json;
select count(*) as count_rows from tables where name = 'test2';
-- 1 row: test2. NOTE that the id has changed from 9048 into 9050
select count(*) as count_rows from keys where table_id in (select id from tables where name = 'test2');
-- 1 row: test2_col1_pkey
select count(*) as count_rows from objects where id in (select id from keys where table_id in (select id from tables where name = 'test2'));
-- 3 rows! which are also identical: col1

ALTER TABLE json.test2 SET SCHEMA sys;
select count(*) as count_rows from tables where name = 'test2';
-- 1 row: test2. NOTE that the id has changed from 9050 into 9052
select count(*) as count_rows from keys where table_id in (select id from tables where name = 'test2');
-- 1 row: test2_col1_pkey
select count(*) as count_rows from objects where id in (select id from keys where table_id in (select id from tables where name = 'test2'));
-- 4 rows! which are also identical: col1

-- Now repeat the invalid FK references exist in the db. All next queries should return zero rows:
SELECT * FROM sys.columns WHERE table_id NOT IN (SELECT id FROM sys.tables);
-- lists 3 invalid rows
SELECT * FROM sys._columns WHERE table_id NOT IN (SELECT id FROM sys._tables);
-- lists 3 invalid rows
SELECT * FROM sys.keys WHERE table_id NOT IN (SELECT id FROM sys.tables);
-- lists 3 invalid rows
SELECT * FROM sys.objects WHERE id NOT IN (SELECT id FROM sys.ids);
-- lists 3 invalid rows
SELECT * FROM sys.idxs WHERE table_id NOT IN (SELECT id FROM sys.tables);
-- lists 3 invalid rows
SELECT * FROM sys.dependencies WHERE id NOT IN (SELECT id FROM sys.ids);
SELECT * FROM sys.dependencies WHERE depend_id NOT IN (SELECT id FROM sys.ids);

drop table sys.test2;

-- Now repeat the invalid FK references exist in the db. All next queries should return zero rows:
SELECT * FROM sys.columns WHERE table_id NOT IN (SELECT id FROM sys.tables);
-- lists 4 invalid rows
SELECT * FROM sys._columns WHERE table_id NOT IN (SELECT id FROM sys._tables);
-- lists 4 invalid rows
SELECT * FROM sys.keys WHERE table_id NOT IN (SELECT id FROM sys.tables);
-- lists 4 invalid rows
SELECT * FROM sys.objects WHERE id NOT IN (SELECT id FROM sys.ids);
-- lists 4 invalid rows
SELECT * FROM sys.idxs WHERE table_id NOT IN (SELECT id FROM sys.tables);
-- lists 4 invalid rows
SELECT * FROM sys.dependencies WHERE id NOT IN (SELECT id FROM sys.ids);
SELECT * FROM sys.dependencies WHERE depend_id NOT IN (SELECT id FROM sys.ids);

-- now try to recreate the dropped table
create table sys.test2 (col1 serial);
-- it produces Error: CONSTRAINT PRIMARY KEY: key test2_col1_pkey already exists SQLState:  42000

drop table if exists sys.test2;

