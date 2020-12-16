-- check if we have an increasing and rotating queue table

-- Clean up sys.queue(): since we're using max_clients=4, seven queries will
--   push out all queriesl automatically executed during the database start up
--   and force sys.queue to rotate since all queries so far are instantly
--   finished.
select 1;
select 2;
select 3;
select 4;
select 5;
select 6;
select 7;
-- We expect the last three queries: sys.queue() keeps (MAL_MAXCLIENTS - 1)
--   queries in its queue so that there is always an empty slot for the next
--   query
-- Have to rely on query text to get determinable results
select username, query from sys.queue() order by query;
