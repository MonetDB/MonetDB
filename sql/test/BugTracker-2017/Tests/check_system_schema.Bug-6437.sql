select name, authorization, owner, "system" from sys.schemas where name IN ('sys', 'tmp', 'json', 'profiler') order by name;

set schema profiler;
select current_schema;

set schema json;
select current_schema;

set schema tmp;
select current_schema;

set schema sys;
select current_schema;

