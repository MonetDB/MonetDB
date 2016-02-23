create table t1 (id int, name varchar(1024), PRIMARY KEY(id));

create table t2 (id int, age int, PRIMARY KEY (ID), FOREIGN KEY(id) REFERENCES t1(id));


ALTER TABLE t1 DROP CONSTRAINT t1_id_pkey CASCADE;

select name from tables where name = 't1';
select name from tables where name = 't2';
select name from keys where name not in ('files_pkey_file_id', 'sq_pkey_sn_file_id', 'sq_fkey_file_id', 'rg_pkey_id_file_id', 'rg_fkey_file_id', 'pg_pkey_id_file_id', 'pg_fkey_file_id', 'spatial_ref_sys_srid_pkey');

--just for debug
--select * from dependencies;

drop table t1;
drop table t2;

--just for debug
--select * from dependencies;


