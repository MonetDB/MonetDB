-- check if we have an increasing queue table

-- Clean up sys.queue(): since we're using max_clients=4, four queries should
-- push out all existing queries, which should have all been finished by now
select 1;
select 2;
select 3;
select 4;
-- We expect the last three queries: sys.queue() keeps (MAL_MAXCLIENTS - 1)
--   queries in its queue so that there is always an empty slot for the next
--   query
-- Have to rely on query text to get determinable results
select username, query from sys.queue() order by query;
