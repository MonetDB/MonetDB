START TRANSACTION;
create schema voc
create table voctest (id int);
select t.name, s.name from sys.tables as t, sys.schemas as s where t.schema_id = s.id and s.name = 'voc';
set schema voc; -- switch schema
