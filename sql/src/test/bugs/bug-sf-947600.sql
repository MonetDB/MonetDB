SELECT   a.id, b.id, COUNT(*)
FROM     tables a, columns ta,
         tables b, columns tb
WHERE    a.id < b.id AND
         a.id = ta.table_id AND
         b.id = tb.table_id AND
         ta.name = tb.name
GROUP BY a.id, b.id
HAVING COUNT(*) >= 1;

select name from tables having 1=1;
select name from tables group by name having 1=1;
