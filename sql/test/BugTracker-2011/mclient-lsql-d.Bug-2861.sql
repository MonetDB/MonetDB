-- this should yield in nothing (empty database)
\d
-- create a table
create table bug2861 (id int);
-- we should see it
\d
drop table bug2861;
-- and it should be gone again
\d
