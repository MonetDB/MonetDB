CREATE USER "monet_test" WITH PASSWORD 'pass_test' NAME 'TEST_USER' SCHEMA "sys";

CREATE SCHEMA "test" AUTHORIZATION "monet_test";

DROP USER monetdb;

ALTER USER "monet_test" SET SCHEMA "test";

DROP SCHEMA test;

