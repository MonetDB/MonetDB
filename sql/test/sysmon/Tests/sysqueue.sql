-- check if we have a increasing queue table
-- the case expression is used to make sure the test is timezone agnostic
select username, status, case when query ilike 'set time%' then 'set time...' else query end from sys.queue();
select count(*) from sys.queue();
select count(*) from sys.queue();
select count(*) from sys.queue();
select count(*) from sys.queue();
select count(*) from sys.queue() where query = 'select count(*) from sys.queue();';
