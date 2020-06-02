create schema qdv;
set schema qdv;
select current_schema;

create user "jtest" with unencrypted password 'wacht01' name 'Jan Test' schema "sys";

select privileges from sys.privileges where auth_id = (select id from sys.auths where name = 'jtest');

create table abem (col1 int);
grant select on abem to "jtest";
select privileges from sys.privileges where auth_id = (select id from sys.auths where name = 'jtest');

grant insert, update, delete, truncate on abem to "jtest";
select privileges from sys.privileges where auth_id = (select id from sys.auths where name = 'jtest');

drop user "jtest";
-- this should also remove the granted privileges in sys.privileges
select privileges from sys.privileges where auth_id = (select id from sys.auths where name = 'jtest');
-- should be empty


-- cleanup
drop table abem;
set schema sys;
select current_schema;
drop schema qdv;

