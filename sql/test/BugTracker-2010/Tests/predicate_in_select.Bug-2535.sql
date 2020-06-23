select (select count(*) from sys._tables) > 5;
select 5 < (select count(*) from sys._tables);
