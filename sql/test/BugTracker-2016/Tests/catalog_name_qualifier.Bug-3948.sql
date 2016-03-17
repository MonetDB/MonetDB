select * from dependency_types where dependency_type_id IN (1, 2);
select * from sys.dependency_types where dependency_type_id IN (1, 2);
select * from sys.sys.dependency_types where dependency_type_id IN (1, 2);
select * from sys.sys.sys.dependency_types;

select * from table_types where table_type_id IN (0, 1);
select * from sys.table_types where table_type_id IN (0, 1);
select * from sys.sys.table_types where table_type_id IN (0, 1);
select * from sys.sys.sys.table_types;

set schema tmp;
create temporary table tmp.t3948 (c1 int) ON COMMIT PRESERVE ROWS;
insert into tmp.t3948 values (10), (20);
select * from tmp.t3948;
select * from t3948;
select * from tmp.tmp.t3948;
select * from tmp.tmp.tmp.t3948;
drop table tmp.t3948;

set schema sys;
create table sys.t3948 (c1 int);
insert into sys.t3948 values (11), (21);
select * from sys.t3948;
select * from t3948;
select * from sys.sys.t3948;
select * from sys.sys.sys.t3948;
drop table sys.t3948;

select * from dependencies_tables_on_views();
select * from sys.dependencies_tables_on_views();
select * from sys.sys.dependencies_tables_on_views();
select * from sys.sys.sys.dependencies_tables_on_views();

