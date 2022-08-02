create schema qdv;
set schema qdv;
select current_schema;

create user "jtest" with unencrypted password 'wacht01' name 'Jan Test' schema "sys";
declare jtest_id int;
select id into jtest_id from sys.auths where name = 'jtest';
select jtest_id > 0;

select privileges from sys.privileges where auth_id = jtest_id;

create table abem (col1 int);
grant select on abem to "jtest";
select privileges from sys.privileges where auth_id = jtest_id;

grant insert, update, delete, truncate on abem to "jtest";
select privileges from sys.privileges where auth_id = jtest_id;

drop user "jtest";
-- this should also remove the granted privileges in sys.privileges
select privileges from sys.privileges where auth_id = jtest_id;
-- should be empty


-- cleanup
drop table abem;
set schema sys;
select current_schema;
drop schema qdv;

