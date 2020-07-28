-- Use prepared statements to force sys.queue() to expand its query circle.
-- This test relies on the little error that once a prepared statement is
--   executed, the "prepare ..." query is not removed from sys.queue()
prepare select 0;
prepare select 1;
prepare select 2;
prepare select 3;
prepare select 4;
prepare select 5;
prepare select 6;
prepare select 7;
select username, status, query from sys.queue();
exec 0();
select 0, username, status, query from sys.queue();
exec 1();
select 1, username, status, query from sys.queue();
exec 2();
select 2, username, status, query from sys.queue();
exec 3();
select 3, username, status, query from sys.queue();
exec 4();
select 4, username, status, query from sys.queue();
exec 5();
exec 6();
exec 7();
select 7, username, status, query from sys.queue();
select 8, username, status, query from sys.queue();
