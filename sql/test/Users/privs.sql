set schema "my_schema";

CREATE table test (i int, b bigint);

-- grant right to my_user not to my_user2
GRANT SELECT on table test to my_user;
GRANT INSERT on table test to my_user;
GRANT UPDATE on table test to my_user;
GRANT DELETE on table test to my_user;
