select name, authorization, owner from sys.schemas where system and name IN ('sys', 'tmp', 'json', 'profiler') order by name;

set schema profiler;
select current_schema;
drop schema profiler;

set schema json;
select current_schema;
drop schema profiler restrict;

select name, authorization, owner from sys.schemas where system and name IN ('sys', 'tmp', 'json', 'profiler') order by name;

set schema profiler;
set schema tmp;
select current_schema;
drop schema json restrict;

select name, authorization, owner from sys.schemas where system and name IN ('sys', 'tmp', 'json', 'profiler') order by name;

set schema json;
set schema sys;
select current_schema;
drop schema tmp restrict;

set schema tmp;
drop schema sys restrict;

select name, authorization, owner from sys.schemas where system and name IN ('sys', 'tmp', 'json', 'profiler') order by name;

set schema profiler;
select current_schema;
set schema json;
select current_schema;
set schema sys;
select current_schema;

select name, authorization, owner, "system" from sys.schemas where name IN ('sys', 'tmp', 'json', 'profiler') order by name;

drop schema profiler cascade;
drop schema json cascade;
drop schema tmp cascade;

select name, authorization, owner, "system" from sys.schemas where name IN ('sys', 'tmp', 'json', 'profiler') order by name;

set schema tmp;
select current_schema;
drop schema sys cascade;

select name, authorization, owner, "system" from sys.schemas where name IN ('sys', 'tmp', 'json', 'profiler') order by name;

