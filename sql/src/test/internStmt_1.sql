START TRANSACTION;
CREATE USER "romulo" WITH PASSWORD 'romulo' NAME 'UserTest' SCHEMA "sys";
CREATE SCHEMA "test_schema" AUTHORIZATION "romulo";
ALTER USER "romulo" SET SCHEMA "test_schema";
commit;
