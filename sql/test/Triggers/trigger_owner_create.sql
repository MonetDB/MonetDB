--test the owner restriction for triggers

create user "user_test" with password 'pass' NAME 'test1_user' SCHEMA "sys";

create table t1 (id int);


