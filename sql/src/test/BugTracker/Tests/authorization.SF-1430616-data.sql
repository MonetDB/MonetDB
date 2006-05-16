CREATE USER "voc" WITH PASSWORD 'voc' NAME 'VOC Explorer' SCHEMA "sys";
select users.name, users.fullname, schemas.name
	from users, schemas
	where users.default_schema = schemas.id;
CREATE SCHEMA "voc" AUTHORIZATION "voc";
ALTER USER "voc" SET SCHEMA "voc";
select users.name, users.fullname, schemas.name
	from sys.users, sys.schemas
	where users.default_schema = schemas.id;
