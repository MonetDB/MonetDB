create table t1715553a(id int DEFAULT 50, name varchar(1024), age int );
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1715553a');
alter table t1715553a alter id set DEFAULT 30;
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1715553a');
alter table t1715553a alter id DROP DEFAULT;
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1715553a');
drop table t1715553a;

create table t1715553a(id int, name varchar(1024), age int );
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1715553a');
alter table t1715553a alter id set NULL;
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1715553a');
drop table t1715553a;

create table t1715553a(id int, name varchar(1024), age int );
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1715553a');
alter table t1715553a alter id set NOT NULL;
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1715553a');
drop table t1715553a;

