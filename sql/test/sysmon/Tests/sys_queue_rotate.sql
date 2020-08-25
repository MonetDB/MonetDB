-- test the rotation of the queue
-- the case expression is used to make sure the test is timezone agnostic
select 1, username, status, case when query ilike 'set time%' then 'set time...' else query end from sys.queue();
select 2, username, status, case when query ilike 'set time%' then 'set time...' else query end from sys.queue();
select 3, username, status, case when query ilike 'set time%' then 'set time...' else query end from sys.queue();
select 4, username, status, case when query ilike 'set time%' then 'set time...' else query end from sys.queue();
select 5, username, status, case when query ilike 'set time%' then 'set time...' else query end from sys.queue();
select 6, username, status, case when query ilike 'set time%' then 'set time...' else query end from sys.queue();
