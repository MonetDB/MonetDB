SELECT   a.name, b.name, COUNT(*)
FROM     tables a, columns ta,
         tables b, columns tb
WHERE    a.id < b.id AND
         a.id = ta.table_id AND
         b.id = tb.table_id AND
         ta.name = tb.name AND
	 a."system" = true AND 
	 b."system" = true 
GROUP BY a.name, b.name
HAVING COUNT(*) >= 1
ORDER BY a.name, b.name;

select name from tables where "istable" = true and "system" = true having 1=1;
select name from tables where "istable" = true and "system" = true having 1=0;

select name from tables where "istable" = true and "system" = true group by name having 1=1;
select name from tables where "istable" = true and "system" = true group by name having 1=0;
