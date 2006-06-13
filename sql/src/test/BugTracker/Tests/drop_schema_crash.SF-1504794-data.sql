CREATE USER "dbtapestry" WITH PASSWORD 'dbtapestry' NAME 'DB Tapestry' SCHEMA "sys";
CREATE SCHEMA "dbtapestry" AUTHORIZATION "dbtapestry";
ALTER USER "dbtapestry" SET SCHEMA "dbtapestry";
DROP schema "dbtapestry";
SELECT count(*) from "users";
