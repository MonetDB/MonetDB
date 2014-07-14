select u.name, u.fullname, s.name as default_schema from sys.users u, sys.schemas s where u.default_schema = s.id and u.name like '%skyserver%';

alter user "skyserver" set schema "sys";
drop schema sky;
drop user skyserver;
