CREATE USER "voc" WITH PASSWORD 'voc' NAME 'VOC Explorer' SCHEMA "sys";
SELECT users.name, users.fullname, schemas.name
	FROM users, schemas
	WHERE users.default_schema = schemas.id;
CREATE SCHEMA "voc" AUTHORIZATION "voc";
ALTER USER "voc" SET SCHEMA "voc";
SELECT users.name, users.fullname, schemas.name
	FROM sys.users, sys.schemas
	WHERE users.default_schema = schemas.id;
