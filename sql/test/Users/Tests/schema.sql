CREATE ROLE my_role;

CREATE SCHEMA my_schema AUTHORIZATION my_role;

CREATE USER my_user with password 'p1' name 'User with role' schema "my_schema";

GRANT my_role to my_user;

CREATE USER my_user2 with password 'p2' name 'User without role' schema "my_schema";
