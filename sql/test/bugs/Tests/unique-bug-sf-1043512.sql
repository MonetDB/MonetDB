-- unique_test whether the server does correct checking of the unique constraint
start transaction;

-- create a table to test on
create table unique_test (
	id int unique
);
-- make sure this table will persist
commit;

-- first check with auto-commit turned off
start transaction;

insert into unique_test values (1);
-- this one should fail!
insert into unique_test values (1);

-- rollback our changes, either because the transaction is aborted, or to make
-- the table clean for the rest of the test
rollback;

-- go checking in auto-commit mode

insert into unique_test values (1);
-- this one should (again) fail!
insert into unique_test values (1);

-- clean up mess we made, we're in auto-commit mode, so no commit required
drop table unique_test;
