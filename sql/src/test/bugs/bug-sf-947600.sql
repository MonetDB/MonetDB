SELECT   a.id, b.id, COUNT(*)
FROM     tables a, columns ta,
         tables b, columns tb
WHERE    a.id < b.id AND
         a.id = ta.table_id AND
         b.id = tb.table_id AND
         ta.name = tb.name AND
	 a."system" = true 
GROUP BY a.id, b.id
HAVING COUNT(*) >= 1;

select name from tables where "istable" = true having 1=1;
select name from tables where "istable" = true having 1=0;

select name from tables where "istable" = true group by name having 1=1;
select name from tables where "istable" = true group by name having 1=0;
