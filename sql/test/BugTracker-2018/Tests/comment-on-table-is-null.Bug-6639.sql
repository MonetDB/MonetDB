select remark from comments where id in (select id from _tables where name = 'abc_6639');
-- shows 0 rows, initially there are no comments set

create table abc_6639 (nr int);
comment on table abc_6639 is 'abc_rem';
select remark from comments where id in (select id from _tables where name = 'abc_6639');
-- shows 1 row as expected
\dt abc_6639

comment on table abc_6639 is null;
select remark from comments where id in (select id from _tables where name = 'abc_6639');
-- shows 1 row but with remark column being null. this is not expected and not allowed as the remark column is defined as NOT NULL
\dt abc_6639

-- show that the comment column id and remark are both set to NOT NULL
select number, name, type, type_digits, "null" from _columns where table_id in (select id from _tables where name = 'comments' and system);
\dt comments

comment on table abc_6639 is '';
select remark from comments where id in (select id from _tables where name = 'abc_6639');
-- shows 0 rows, the row is now deleted as expected
\dt abc_6639

comment on table abc_6639 is null;
select remark from comments where id in (select id from _tables where name = 'abc_6639');
-- shows 1 row! So it is created again but with remark column being null
\dt abc_6639

comment on table abc_6639 is 'abc_rem2';
select remark from comments where id in (select id from _tables where name = 'abc_6639');
-- shows 1 row as expected
\dt abc_6639

drop table abc_6639;
select remark from comments where id in (select id from _tables where name = 'abc_6639');
-- shows 0 rows, the row is deleted as expected (implicitly deleted by the drop table statement)
\dt abc_6639

