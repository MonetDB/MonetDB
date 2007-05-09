create table t1(id int DEFAULT 50, name varchar(1024), age int );
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1');
alter table t1 alter id set DEFAULT 30;
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1');
alter table t1 alter id DROP DEFAULT;
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1');
drop table t1;

create table t1(id int, name varchar(1024), age int );
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1');
alter table t1 alter id set NULL;
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1');
drop table t1;

create table t1(id int, name varchar(1024), age int );
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1');
alter table t1 alter id set NOT NULL;
select name, "default", "null" from columns where name = 'id' and table_id = (select id from tables where name = 't1');
drop table t1;

