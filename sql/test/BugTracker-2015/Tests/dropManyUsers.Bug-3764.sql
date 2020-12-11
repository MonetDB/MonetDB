-- Create more than 2 users (i.e. 4) and drop all of them
CREATE SCHEMA schema3764;
SELECT u.name, u.fullname, s.name as schema FROM "sys"."users" u JOIN "sys"."schemas" s ON u.default_schema = s.id AND not s.system;
CREATE USER user1 with password '1' name '1st user' schema schema3764;
CREATE USER user2 with password '2' name '2nd user' schema schema3764;
CREATE USER user3 with password '3' name '3rd user' schema schema3764;
CREATE USER user4 with password '4' name '4th user' schema schema3764;
SELECT u.name, u.fullname, s.name as schema FROM "sys"."users" u JOIN "sys"."schemas" s ON u.default_schema = s.id AND not s.system;
DROP USER user1;
DROP USER user2;
DROP USER user3;
DROP USER user4;
SELECT u.name, u.fullname, s.name as schema FROM "sys"."users" u JOIN "sys"."schemas" s ON u.default_schema = s.id AND not s.system;
DROP SCHEMA schema3764;

