-- check if we have a increasing queue table
select username, status, query from sys.queue();
select count(*) from sys.queue();
select count(*) from sys.queue();
select count(*) from sys.queue();
select count(*) from sys.queue();
select count(*) from sys.queue() where query = 'select count(*) from sys.queue();';
