
CREATE USER user_a WITH PASSWORD 'user_a' NAME 'User A' SCHEMA sys;
CREATE USER user_b WITH PASSWORD 'user_b' NAME 'User B' SCHEMA sys;
CREATE ROLE role_b;
GRANT role_b to user_b;

CREATE SCHEMA schema_a AUTHORIZATION user_a;
CREATE SCHEMA schema_b AUTHORIZATION role_b;

CREATE TABLE schema_a.tab_a(i INTEGER);
CREATE TABLE schema_b.tab_b(i INTEGER);

COMMENT ON SCHEMA schema_a IS 'set by super user';
COMMENT ON SCHEMA schema_b IS 'set by super user';
