START TRANSACTION;

CREATE USER "skyserver" WITH PASSWORD 'skyserver' NAME 'SkyServer' SCHEMA "sys";
CREATE SCHEMA "skyserver" AUTHORIZATION "skyserver";
ALTER USER "skyserver" SET SCHEMA "skyserver";

COMMIT;
