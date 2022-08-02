select (select count(*) from _tables) > 5;
select 5 < (select count(*) from _tables);
