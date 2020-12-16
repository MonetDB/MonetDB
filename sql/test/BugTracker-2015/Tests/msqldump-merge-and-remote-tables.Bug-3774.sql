CREATE TABLE t1 (i int);
CREATE MERGE TABLE mt1 (t int);
ALTER TABLE mt1 ADD TABLE t1;
CREATE REMOTE  TABLE rt1 (t int)  on 'mapi:monetdb://localhost:50000/test';
with describe_all_objects as (
 select s.name as sname,
     t.name,
     s.name || '.' || t.name as fullname,
     cast(case t.type
     when 1 then 2
     else 1
     end as smallint) as ntype,
     (case when t.system then 'SYSTEM ' else '' end) || tt.table_type_name as type,
     t.system
   from sys._tables t
   left outer join sys.schemas s on t.schema_id = s.id
   left outer join sys.table_types tt on t.type = tt.table_type_id )
select type, fullname from describe_all_objects where (ntype & 3) > 0 and not system and (sname is null or sname = current_schema) order by fullname, type;
select "name", "query", "type", "remark" from describe_table('sys', 'mt1');
select "name", "query", "type", "remark" from describe_table('sys', 'rt1');

DROP TABLE rt1;
DROP TABLE mt1;
DROP TABLE t1;
