
create table x (s string);
insert into x values('%able%');
select name, schema_id, query, type, system, commit_action, readonly, s from sys._tables, x where name like s;
drop table x;
