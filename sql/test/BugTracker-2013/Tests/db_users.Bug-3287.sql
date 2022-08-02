create user my_user with password 'p1' name 'User with role' schema "sys";
select name from "sys".users where lower(name) = 'monetdb';
