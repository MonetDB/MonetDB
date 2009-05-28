select row_number() over (order by tables) as row, '1' limit 10;
select row_number() over (order by name) as row, '1' from tables limit 10;
